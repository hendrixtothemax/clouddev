import argparse
from datetime import date
import io
from pathlib import Path
import sys
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
LOG_ROOT = PROJECT_ROOT / "consumer-logs" / "unit-tests"


class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, text):
        for stream in self.streams:
            stream.write(text)
            stream.flush()
        return len(text)

    def flush(self):
        for stream in self.streams:
            stream.flush()


def next_log_path(today: date) -> Path:
    month_directory = LOG_ROOT / today.strftime("%Y-%m")
    month_directory.mkdir(parents=True, exist_ok=True)
    base = f"consumer-{today.month:02d}-{today.day:02d}-{today.year}"
    version = 1
    while True:
        log_path = month_directory / f"{base}-v{version}.log"
        if not log_path.exists():
            return log_path
        version += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Consumer unit tests")
    parser.add_argument(
        "--suite",
        choices=("offline", "online", "all"),
        default="offline",
        help="Test suite to run (default: offline)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    patterns = {
        "offline": "test_consumer_offline.py",
        "online": "test_consumer_online.py",
        "all": "test_consumer_*.py",
    }
    suite = unittest.defaultTestLoader.discover(
        start_dir=str(TESTS_DIR),
        pattern=patterns[args.suite],
        top_level_dir=str(PROJECT_ROOT),
    )

    log_path = next_log_path(date.today())
    with log_path.open("w", encoding="utf-8") as log_file:
        runner = unittest.TextTestRunner(
            stream=Tee(sys.stdout, log_file),
            verbosity=2,
        )
        result = runner.run(suite)

    print(f"Test log saved to {log_path}")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
