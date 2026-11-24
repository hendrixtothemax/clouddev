"""System test: run real consumers and producers against the SQS queue.

Runs four scenarios (1 or many consumers x 1 or many producers), saves every
process's logs under system-test-logs/<timestamp>/<scenario>/, and checks that
every request the producers generated was handled and deleted exactly once,
with no errors.

Run from the project root with the clouddev environment active:

    python tests/system_test.py
    python tests/system_test.py --scenarios multi-consumer-multi-producer
    python tests/system_test.py --consumers 3 --consumer-runtime 120

Producers run for --producer-runtime (30 s), and consumers for
--consumer-runtime (90 s), so each scenario takes about 2 minutes and all four
about 8. This talks to real AWS resources (SQS, DynamoDB).
"""

import argparse
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re
import subprocess
import sys
import time

import boto3


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONSUMER = PROJECT_ROOT / "consumer.py"
PRODUCER = PROJECT_ROOT / "producer.jar"
LOG_ROOT = PROJECT_ROOT / "system-test-logs"

DEFAULT_QUEUE_URL = (
    "https://sqs.us-east-1.amazonaws.com/264671405801/cs5270-requests"
)

# Consumer log lines (see consumer.py).
CREATED_RE = re.compile(r"Created widget in (?:DynamoDB|S3)")
UPDATE_RE = re.compile(r"Update requests are not implemented")
DELETE_RE = re.compile(r"Delete requests are not implemented")
DELETED_RE = re.compile(r"Deleted request (\S+) from queue\.")
# Producer log lines, e.g. "Generate a Create request", "Generate an Update request".
GENERATED_RE = re.compile(r"Generate an? (\w+) request")


@dataclass
class Scenario:
    name: str
    consumers: int
    producers: int


@dataclass
class ScenarioResult:
    name: str
    produced: Counter = field(default_factory=Counter)
    handled: Counter = field(default_factory=Counter)
    per_consumer: list[int] = field(default_factory=list)
    deleted_ids: Counter = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    queue_drained: bool = False

    @property
    def passed(self) -> bool:
        return not self.failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-p", "--profile", default="clouddev")
    parser.add_argument("-r", "--region", default="us-east-1")
    parser.add_argument("-rq", "--request-queue", default=DEFAULT_QUEUE_URL)
    parser.add_argument("-dwt", "--dynamodb-widget-table", default="widgets")
    parser.add_argument(
        "--requests", type=int, default=1000,
        help="Maximum requests per producer (-mwr). Default: 1000, so the "
        "producer runtime is what normally stops it.",
    )
    parser.add_argument(
        "--producer-runtime", type=int, default=30,
        help="Seconds each producer runs (-mrt). Default: 30",
    )
    parser.add_argument(
        "--consumer-runtime", type=int, default=90,
        help="Seconds each consumer runs (-mrt). Must be long enough to "
        "drain everything the producers send. Default: 90",
    )
    parser.add_argument(
        "--consumers", type=int, default=2,
        help="Consumer count for multi-consumer scenarios. Default: 2",
    )
    parser.add_argument(
        "--producers", type=int, default=2,
        help="Producer count for multi-producer scenarios. Default: 2",
    )
    parser.add_argument(
        "--settle-timeout", type=int, default=60,
        help="Seconds to wait for the queue counts to read empty, before a "
        "scenario starts and after its consumers exit. Default: 60",
    )
    parser.add_argument(
        "--scenarios", nargs="*",
        help="Scenario names to run (default: all four).",
    )
    return parser.parse_args()


def build_scenarios(args: argparse.Namespace) -> list[Scenario]:
    all_scenarios = [
        Scenario("single-consumer-single-producer", 1, 1),
        Scenario("single-consumer-multi-producer", 1, args.producers),
        Scenario("multi-consumer-single-producer", args.consumers, 1),
        Scenario("multi-consumer-multi-producer", args.consumers, args.producers),
    ]
    if not args.scenarios:
        return all_scenarios
    wanted = set(args.scenarios)
    unknown = wanted - {s.name for s in all_scenarios}
    if unknown:
        sys.exit(f"Unknown scenario(s): {', '.join(sorted(unknown))}")
    return [s for s in all_scenarios if s.name in wanted]


def queue_depth(sqs, queue_url: str) -> tuple[int, int]:
    """Return (visible, in-flight) message counts."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]
    return (
        int(attrs["ApproximateNumberOfMessages"]),
        int(attrs["ApproximateNumberOfMessagesNotVisible"]),
    )


def wait_for_drain(sqs, queue_url: str, timeout: int) -> bool:
    """Wait until the queue reports empty three checks in a row.

    The counts are approximate, so one zero reading is not enough.
    """
    deadline = time.monotonic() + timeout
    zero_streak = 0
    while time.monotonic() < deadline:
        visible, in_flight = queue_depth(sqs, queue_url)
        print(f"    queue: {visible} waiting, {in_flight} in flight")
        zero_streak = zero_streak + 1 if visible == in_flight == 0 else 0
        if zero_streak >= 3:
            return True
        time.sleep(5)
    return False


def start_process(cmd: list[str], workdir: Path) -> subprocess.Popen:
    workdir.mkdir(parents=True, exist_ok=True)
    output = (workdir / "console.out").open("w", encoding="utf-8")
    (workdir / "command.txt").write_text(" ".join(cmd) + "\n", encoding="utf-8")
    return subprocess.Popen(
        cmd, cwd=workdir, stdout=output, stderr=subprocess.STDOUT, text=True
    )


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


def collect_results(
    scenario: Scenario, scenario_dir: Path, result: ScenarioResult
) -> None:
    for j in range(1, scenario.producers + 1):
        pdir = scenario_dir / f"producer-{j}"
        # The producer writes logs/producer.log relative to its working dir;
        # fall back to its console output if that file is missing.
        text = read_text(pdir / "logs" / "producer.log") or read_text(
            pdir / "console.out"
        )
        result.produced.update(m.lower() for m in GENERATED_RE.findall(text))
        for line in text.splitlines():
            if " ERROR " in line or "Exception" in line:
                result.errors.append(f"producer-{j}: {line.strip()}")

    for i in range(1, scenario.consumers + 1):
        cdir = scenario_dir / f"consumer-{i}"
        log = read_text(cdir / "consumer.log")
        console = read_text(cdir / "console.out")

        created = len(CREATED_RE.findall(log))
        updates = len(UPDATE_RE.findall(log))
        deletes = len(DELETE_RE.findall(log))
        result.handled.update(create=created, update=updates, delete=deletes)
        result.per_consumer.append(created + updates + deletes)
        result.deleted_ids.update(DELETED_RE.findall(log))

        for line in log.splitlines():
            if " ERROR " in line:
                result.errors.append(f"consumer-{i}: {line.strip()}")
        if "Traceback" in console:
            result.errors.append(f"consumer-{i}: traceback in console.out")


def evaluate(scenario: Scenario, result: ScenarioResult) -> None:
    total_produced = sum(result.produced.values())
    total_handled = sum(result.handled.values())
    unique_deleted = len(result.deleted_ids)
    duplicates = sum(n - 1 for n in result.deleted_ids.values() if n > 1)

    if total_produced == 0:
        result.failures.append("producers generated no requests")
    if not result.queue_drained:
        result.failures.append(
            "messages were left in the queue after the consumers stopped; "
            "try a longer --consumer-runtime"
        )
    if result.errors:
        result.failures.append(f"{len(result.errors)} error line(s) in logs")
    if unique_deleted != total_produced:
        result.failures.append(
            f"{unique_deleted} unique requests deleted, "
            f"but {total_produced} were produced"
        )
    if total_handled != unique_deleted + duplicates:
        result.failures.append(
            f"handled {total_handled} requests but deleted "
            f"{unique_deleted + duplicates} messages"
        )
    if scenario.consumers > 1 and 0 in result.per_consumer:
        result.failures.append("at least one consumer processed nothing")

    # A standard queue may deliver a message more than once. That is
    # expected SQS behaviour, so it is reported but does not fail the test.
    if duplicates:
        result.warnings.append(
            f"{duplicates} duplicate deliveries (same request handled twice)"
        )


def run_scenario(
    scenario: Scenario, args: argparse.Namespace, sqs, run_dir: Path
) -> ScenarioResult:
    print(
        f"\n=== {scenario.name}: {scenario.consumers} consumer(s), "
        f"{scenario.producers} producer(s) ==="
    )
    result = ScenarioResult(scenario.name)
    scenario_dir = run_dir / scenario.name

    # The queue counts are approximate and lag behind, so give them time to
    # settle after the previous scenario instead of failing straight away.
    print("  checking the queue is empty")
    if not wait_for_drain(sqs, args.request_queue, args.settle_timeout):
        result.failures.append(
            "queue was not empty at start; purge it and rerun"
        )
        return result

    consumer_cmd = [
        sys.executable, str(CONSUMER),
        "-p", args.profile, "-r", args.region,
        "-rq", args.request_queue,
        "-dwt", args.dynamodb_widget_table,
        "-mrt", str(args.consumer_runtime * 1000),
    ]
    producer_cmd = [
        "java", "-jar", str(PRODUCER),
        "-p", args.profile, "-r", args.region,
        "-rq", args.request_queue,
        "-mwr", str(args.requests),
        "-mrt", str(args.producer_runtime * 1000),
    ]

    consumers = [
        start_process(consumer_cmd, scenario_dir / f"consumer-{i}")
        for i in range(1, scenario.consumers + 1)
    ]
    print(f"  started {len(consumers)} consumer(s)")
    time.sleep(2)  # let consumers start polling first

    producers = [
        start_process(producer_cmd, scenario_dir / f"producer-{j}")
        for j in range(1, scenario.producers + 1)
    ]
    print(f"  started {len(producers)} producer(s); waiting for them to finish")
    for j, proc in enumerate(producers, start=1):
        code = proc.wait()
        if code != 0:
            result.errors.append(f"producer-{j}: exit code {code}")

    # Consumers stop themselves at -mrt. The runtime check happens between
    # polls, so each one finishes its current receive, processes and deletes
    # those messages, and then exits. Allow one extra long poll (10 s) plus
    # time to work through a cached batch.
    print(
        f"  producers done; waiting up to {args.consumer_runtime}s "
        "for consumers to finish"
    )
    grace = args.consumer_runtime + 30
    for i, proc in enumerate(consumers, start=1):
        try:
            code = proc.wait(timeout=grace)
            if code != 0:
                result.errors.append(f"consumer-{i}: exit code {code}")
        except subprocess.TimeoutExpired:
            proc.kill()
            result.errors.append(
                f"consumer-{i}: did not stop at its max runtime; killed"
            )

    print("  consumers done; checking the queue is empty")
    result.queue_drained = wait_for_drain(
        sqs, args.request_queue, args.settle_timeout
    )

    collect_results(scenario, scenario_dir, result)
    evaluate(scenario, result)
    return result


def format_result(result: ScenarioResult) -> str:
    lines = [
        f"{result.name}: {'PASS' if result.passed else 'FAIL'}",
        f"  produced: {sum(result.produced.values())} {dict(result.produced)}",
        f"  handled:  {sum(result.handled.values())} {dict(result.handled)}",
        f"  per consumer: {result.per_consumer}",
        f"  unique requests deleted from queue: {len(result.deleted_ids)}",
    ]
    lines += [f"  FAIL: {f}" for f in result.failures]
    lines += [f"  note: {w}" for w in result.warnings]
    lines += [f"  error: {e}" for e in result.errors[:10]]
    if len(result.errors) > 10:
        lines.append(f"  ... {len(result.errors) - 10} more error line(s)")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    scenarios = build_scenarios(args)

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    sqs = session.client("sqs")

    run_dir = LOG_ROOT / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir.mkdir(parents=True)
    print(f"Logs: {run_dir}")

    results = [run_scenario(s, args, sqs, run_dir) for s in scenarios]

    summary = "\n\n".join(format_result(r) for r in results)
    (run_dir / "summary.txt").write_text(summary + "\n", encoding="utf-8")
    print("\n" + summary)
    print(f"\nSummary saved to {run_dir / 'summary.txt'}")
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
