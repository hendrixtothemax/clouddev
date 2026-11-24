import argparse
from dataclasses import dataclass
import json
import logging
import time
from typing import Any
from abc import ABC, abstractmethod

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

@dataclass
class WidgetAttribute:
    name: str
    value: str


@dataclass
class WidgetRequest:
    request_type: str
    request_id: str
    widget_id: str
    owner: str
    label: str | None = None
    description: str | None = None
    other_attributes: list[WidgetAttribute] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WidgetRequest":
        request_type = data["type"].lower()
        if request_type not in {"create", "update", "delete"}:
            raise ValueError(f"Unsupported request type: {request_type}")

        attributes = [
            WidgetAttribute(name=item["name"], value=item["value"])
            for item in data.get("otherAttributes", [])
        ]

        return cls(
            request_type=request_type,
            request_id=data["requestId"],
            widget_id=data["widgetId"],
            owner=data["owner"],
            label=data.get("label"),
            description=data.get("description"),
            other_attributes=attributes or None,
        )

# Readers
class RequestReader(ABC):
    """A source of widget requests (an S3 bucket or an SQS queue).

    Callers fetch with get_next_request(), process the request, and then
    call delete_request() so the source does not hand it out again.
    """

    @abstractmethod
    def get_next_request(self) -> WidgetRequest | None:
        """Return the next request, or None if none are available."""

    @abstractmethod
    def delete_request(self, request: WidgetRequest) -> None:
        """Remove a processed request from its source."""

    @staticmethod
    def parse_request(raw_body: str | bytes) -> WidgetRequest | None:
        """Parse a JSON request body, or return None if it is malformed."""
        try:
            return WidgetRequest.from_dict(json.loads(raw_body))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            logging.error("Discarding malformed request: %s", error)
            return None


class S3RequestReader(RequestReader):
    """Reads one request object at a time from an S3 bucket."""

    def __init__(self, client: BaseClient, bucket: str) -> None:
        if not bucket or not bucket.strip():
            raise ValueError("A request bucket name is required.")
        self.client = client
        self.bucket = bucket
        # request_id -> S3 key, so delete_request knows which object to remove.
        self._keys: dict[str, str] = {}

    def get_next_request(self) -> WidgetRequest | None:
        response = self.client.list_objects_v2(Bucket=self.bucket, MaxKeys=1)
        contents = response.get("Contents", [])
        if not contents:
            return None

        key = contents[0]["Key"]
        s3_object = self.client.get_object(Bucket=self.bucket, Key=key)
        try:
            body = s3_object["Body"].read()
        finally:
            s3_object["Body"].close()

        request = self.parse_request(body)
        if request is None:
            # Remove it now, or the next listing returns the same bad object.
            self.client.delete_object(Bucket=self.bucket, Key=key)
            logging.error("Deleted malformed request s3://%s/%s", self.bucket, key)
            return None

        self._keys[request.request_id] = key
        logging.info(
            "Retrieved request %s from s3://%s/%s",
            request.request_id, self.bucket, key,
        )
        return request

    def delete_request(self, request: WidgetRequest) -> None:
        key = self._keys.pop(request.request_id)
        self.client.delete_object(Bucket=self.bucket, Key=key)
        logging.info("Deleted request object s3://%s/%s", self.bucket, key)


class SqsRequestReader(RequestReader):
    """Reads requests from an SQS queue in batches of up to 10.

    One receive_message call (with long polling) fills a local cache, and
    get_next_request hands the cached requests out one at a time. Each
    request's receipt handle is kept so delete_request can remove the
    message after it has been processed.
    """

    MAX_BATCH_SIZE = 10  # SQS limit for receive_message

    def __init__(
        self, client: BaseClient, queue_url: str, wait_seconds: int = 10
    ) -> None:
        if not queue_url or not queue_url.strip():
            raise ValueError("A request queue URL is required.")
        self.client = client
        self.queue_url = queue_url
        self.wait_seconds = wait_seconds
        self._cache: list[tuple[WidgetRequest, str]] = []
        self._receipt_handles: dict[str, str] = {}

    def get_next_request(self) -> WidgetRequest | None:
        if not self._cache:
            self._fill_cache()
        if not self._cache:
            return None

        request, receipt_handle = self._cache.pop(0)
        self._receipt_handles[request.request_id] = receipt_handle
        return request

    def delete_request(self, request: WidgetRequest) -> None:
        receipt_handle = self._receipt_handles.pop(request.request_id, None)
        if receipt_handle is None:
            logging.warning(
                "No receipt handle for request %s; nothing to delete.",
                request.request_id,
            )
            return
        self._delete_message(receipt_handle)
        logging.info("Deleted request %s from queue.", request.request_id)

    def _fill_cache(self) -> None:
        """Receive up to 10 messages and cache the ones that parse."""
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.MAX_BATCH_SIZE,
            WaitTimeSeconds=self.wait_seconds,
        )
        messages = response.get("Messages", [])
        if messages:
            logging.info("Received %d message(s) from queue.", len(messages))

        for message in messages:
            receipt_handle = message["ReceiptHandle"]
            request = self.parse_request(message.get("Body", ""))
            if request is None:
                # No dead-letter queue is configured, so a malformed message
                # would otherwise be redelivered forever.
                self._delete_message(receipt_handle)
                logging.error(
                    "Deleted malformed message %s from queue.",
                    message.get("MessageId"),
                )
                continue
            self._cache.append((request, receipt_handle))

    def _delete_message(self, receipt_handle: str) -> None:
        try:
            self.client.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
            )
        except ClientError as error:
            # Usually an expired receipt handle (visibility timeout passed);
            # the message will be redelivered and handled again.
            logging.error("Failed to delete message from queue: %s", error)


# Request Processing
def widget_as_json_dict(request: WidgetRequest) -> dict[str, Any]:
    """Return the widget properties using the request schema's field names."""
    widget: dict[str, Any] = {
        "widgetId": request.widget_id,
        "owner": request.owner,
    }

    if request.label is not None:
        widget["label"] = request.label
    if request.description is not None:
        widget["description"] = request.description
    if request.other_attributes:
        widget["otherAttributes"] = [
            {"name": attribute.name, "value": attribute.value}
            for attribute in request.other_attributes
        ]

    return widget


def widget_s3_key(request: WidgetRequest) -> str:
    """Return the S3 key a widget is stored under: widgets/{owner}/{id}."""
    owner_prefix = request.owner.replace(" ", "-").lower()
    return f"widgets/{owner_prefix}/{request.widget_id}"


def handle_create_request(
    request: WidgetRequest,
    *,
    widget_s3_client: BaseClient | None = None,
    widget_bucket: str | None = None,
    dynamodb_table: Any = None,
) -> None:
    """Store a widget in the selected S3 bucket or DynamoDB table."""
    if widget_bucket is not None:
        if widget_s3_client is None:
            raise ValueError("An S3 client is required for S3 widget storage.")

        key = widget_s3_key(request)
        body = json.dumps(widget_as_json_dict(request))

        widget_s3_client.put_object(
            Bucket=widget_bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        logging.info("Created widget in S3: s3://%s/%s", widget_bucket, key)
        return

    if dynamodb_table is not None:
        item: dict[str, str] = {
            "id": request.widget_id,
            "widget_id": request.widget_id,
            "owner": request.owner,
        }

        if request.label is not None:
            item["label"] = request.label
        if request.description is not None:
            item["description"] = request.description
        if request.other_attributes:
            for attribute in request.other_attributes:
                item[attribute.name] = attribute.value

        dynamodb_table.put_item(Item=item)
        logging.info("Created widget in DynamoDB: %s", request.widget_id)
        return

    raise ValueError("No widget storage destination was configured.")


# DynamoDB key attributes that an update must never change.
PROTECTED_DYNAMODB_ATTRIBUTES = {"id", "widget_id"}


def merge_widget_update(
    widget: dict[str, Any], request: WidgetRequest
) -> dict[str, Any]:
    """Return a copy of a stored S3 widget with the update applied.

    Fields in the request replace the stored values. An attribute whose new
    value is an empty string is removed; other attributes are added or
    replaced. Attributes the request does not mention are left alone.
    """
    updated = dict(widget)
    updated["owner"] = request.owner
    if request.label is not None:
        updated["label"] = request.label
    if request.description is not None:
        updated["description"] = request.description

    if request.other_attributes:
        attributes = {
            item["name"]: item["value"]
            for item in widget.get("otherAttributes", [])
        }
        for attribute in request.other_attributes:
            if attribute.value == "":
                attributes.pop(attribute.name, None)
            else:
                attributes[attribute.name] = attribute.value
        if attributes:
            updated["otherAttributes"] = [
                {"name": name, "value": value}
                for name, value in attributes.items()
            ]
        else:
            updated.pop("otherAttributes", None)

    return updated


def build_dynamodb_update(request: WidgetRequest) -> dict[str, Any]:
    """Build the update_item arguments (minus Key) for an update request.

    Uses placeholders (#n0, :v0) for every name and value, because attribute
    names such as "width-unit" are not valid in a DynamoDB expression.
    The condition makes the update fail if the widget does not exist,
    rather than quietly creating a partial widget.
    """
    fields: dict[str, str] = {"owner": request.owner}
    if request.label is not None:
        fields["label"] = request.label
    if request.description is not None:
        fields["description"] = request.description

    removals: list[str] = []
    for attribute in request.other_attributes or []:
        if attribute.name in PROTECTED_DYNAMODB_ATTRIBUTES:
            logging.warning(
                "Ignoring attribute %r in request %s: it is a key attribute.",
                attribute.name,
                request.request_id,
            )
            continue
        if attribute.value == "":
            removals.append(attribute.name)
            fields.pop(attribute.name, None)
        else:
            fields[attribute.name] = attribute.value
            if attribute.name in removals:
                removals.remove(attribute.name)

    names: dict[str, str] = {"#id": "id"}
    values: dict[str, str] = {}
    set_parts: list[str] = []
    remove_parts: list[str] = []

    for index, (name, value) in enumerate(fields.items()):
        names[f"#n{index}"] = name
        values[f":v{index}"] = value
        set_parts.append(f"#n{index} = :v{index}")

    for index, name in enumerate(removals):
        names[f"#r{index}"] = name
        remove_parts.append(f"#r{index}")

    expression = "SET " + ", ".join(set_parts)
    if remove_parts:
        expression += " REMOVE " + ", ".join(remove_parts)

    return {
        "UpdateExpression": expression,
        "ConditionExpression": "attribute_exists(#id)",
        "ExpressionAttributeNames": names,
        "ExpressionAttributeValues": values,
    }


def handle_update_request(
    request: WidgetRequest,
    *,
    widget_s3_client: BaseClient | None = None,
    widget_bucket: str | None = None,
    dynamodb_table: Any = None,
) -> bool:
    """Update an existing widget in S3 or DynamoDB.

    Returns True if the widget was updated, or False if it does not exist.
    A missing widget is logged as a warning, not an error: with a standard
    SQS queue an update can arrive before its create, or after a delete.
    """
    if widget_bucket is not None:
        if widget_s3_client is None:
            raise ValueError("An S3 client is required for S3 widget storage.")

        key = widget_s3_key(request)
        try:
            s3_object = widget_s3_client.get_object(Bucket=widget_bucket, Key=key)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {
                "NoSuchKey", "404",
            }:
                logging.warning(
                    "Update failed: widget %s does not exist "
                    "(s3://%s/%s, request ID: %s).",
                    request.widget_id, widget_bucket, key, request.request_id,
                )
                return False
            raise

        try:
            widget = json.loads(s3_object["Body"].read())
        finally:
            s3_object["Body"].close()

        widget_s3_client.put_object(
            Bucket=widget_bucket,
            Key=key,
            Body=json.dumps(merge_widget_update(widget, request)),
            ContentType="application/json",
        )
        logging.info("Updated widget in S3: s3://%s/%s", widget_bucket, key)
        return True

    if dynamodb_table is not None:
        try:
            dynamodb_table.update_item(
                Key={"id": request.widget_id},
                **build_dynamodb_update(request),
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == (
                "ConditionalCheckFailedException"
            ):
                logging.warning(
                    "Update failed: widget %s does not exist in DynamoDB "
                    "(request ID: %s).",
                    request.widget_id, request.request_id,
                )
                return False
            raise
        logging.info("Updated widget in DynamoDB: %s", request.widget_id)
        return True

    raise ValueError("No widget storage destination was configured.")


def handle_delete_request(request: WidgetRequest) -> None:
    logging.warning(
        "Delete requests are not implemented yet (request ID: %s).",
        request.request_id,
    )


def process_requests(
    requests: list[WidgetRequest],
    *,
    widget_s3_client: BaseClient | None = None,
    widget_bucket: str | None = None,
    dynamodb_table: Any = None,
) -> None:
    for request in requests:
        if request.request_type == "create":
            handle_create_request(
                request,
                widget_s3_client=widget_s3_client,
                widget_bucket=widget_bucket,
                dynamodb_table=dynamodb_table,
            )
        elif request.request_type == "update":
            handle_update_request(
                request,
                widget_s3_client=widget_s3_client,
                widget_bucket=widget_bucket,
                dynamodb_table=dynamodb_table,
            )
        elif request.request_type == "delete":
            handle_delete_request(request)
        else:
            logging.warning(
                "Unsupported request type %r (request ID: %s).",
                request.request_type,
                request.request_id,
            )


def run_consumer_loop(
    reader: RequestReader,
    *,
    widget_s3_client: BaseClient | None = None,
    widget_bucket: str | None = None,
    dynamodb_table: Any = None,
    max_runtime_seconds: float = 0,
    idle_sleep_seconds: float = 0.1,
    report_interval_seconds: float = 5,
) -> int:
    """Fetch, process and delete requests until the runtime limit is reached.

    Returns the number of requests processed (useful for tests).
    """
    start_time = time.monotonic()
    last_report = start_time
    processed = 0

    while True:
        now = time.monotonic()
        if max_runtime_seconds > 0 and now - start_time >= max_runtime_seconds:
            logging.info("Maximum runtime reached; stopping consumer.")
            break

        if now - last_report >= report_interval_seconds:
            print(f"Processed requests: {processed}")
            last_report = now

        request = reader.get_next_request()
        if request is None:
            # SQS long polling already waits; this mainly throttles S3 listing.
            time.sleep(idle_sleep_seconds)
            continue

        try:
            process_requests(
                [request],
                widget_s3_client=widget_s3_client,
                widget_bucket=widget_bucket,
                dynamodb_table=dynamodb_table,
            )
            processed += 1
        except Exception:
            logging.exception(
                "Failed to process request %s", request.request_id
            )
        # The request is deleted even if processing failed: there is no
        # dead-letter queue, so a request that always fails would otherwise
        # come back forever.
        reader.delete_request(request)

    return processed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consumer for widget processing"
    )
    parser.add_argument(
        "-p",
        "--profile",
        default=None,
        help="AWS profile to use. If omitted, boto3's default lookup is used "
        "(AWS_* environment variables, then the default profile), which is "
        "what a container needs.",
    )
    parser.add_argument(
        "-r",
        "--region",
        default="us-east-1",
        help="AWS region (default: us-east-1)",
    )
    parser.add_argument(
        "-mrt",
        "--max-runtime",
        type=int,
        default=0,
        help="Maximum runtime in milliseconds; 0 means no maximum",
    )

    request_source = parser.add_mutually_exclusive_group(required=True)
    request_source.add_argument(
        "-rb",
        "--request-bucket",
        help="S3 bucket containing widget requests",
    )
    request_source.add_argument(
        "-rq",
        "--request-queue",
        type=str,
        help="URL of queue that will contain requests (default=None)"
    )

    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument(
        "-wb",
        "--widget-bucket",
        help="Separate S3 bucket in which to store widgets",
    )
    destination.add_argument(
        "-dwt",
        "--dynamodb-widget-table",
        help="DynamoDB table in which to store widgets",
    )

    parser.add_argument(
        "-wkp",
        "--widget-key-prefix",
        default="widgets/",
        help="S3 key prefix for widgets (default: widgets/)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        filename="consumer.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    session = boto3.Session(
        profile_name=args.profile,
        region_name=args.region,
    )
    reader: RequestReader
    if args.request_queue:
        reader = SqsRequestReader(session.client("sqs"), args.request_queue)
    else:
        reader = S3RequestReader(session.client("s3"), args.request_bucket)

    widget_s3_client = None
    dynamodb_table = None

    if args.widget_bucket:
        widget_s3_client = session.client("s3")
    else:
        dynamodb_table = session.resource("dynamodb").Table(
            args.dynamodb_widget_table
        )

    # Use the requested prefix while retaining the required owner/widget ID
    # key structure.
    key_prefix = args.widget_key_prefix.strip("/")
    if not key_prefix:
        parser.error("--widget-key-prefix must not be empty.")
    # The handler's default key structure is widgets/{owner}/{widget_id}.
    # Override its prefix if a different prefix was supplied.
    if key_prefix != "widgets":
        logging.warning(
            "The requested key prefix %r is not used; widget keys use "
            "the required widgets/{owner}/{widget_id} format.",
            args.widget_key_prefix,
        )

    max_runtime_seconds = args.max_runtime / 1000

    print('Starting Consumer Loop...\n')
    run_consumer_loop(
        reader,
        widget_s3_client=widget_s3_client,
        widget_bucket=args.widget_bucket,
        dynamodb_table=dynamodb_table,
        max_runtime_seconds=max_runtime_seconds,
    )


if __name__ == "__main__":
    main()
