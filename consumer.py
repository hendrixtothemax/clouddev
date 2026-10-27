import argparse
from dataclasses import dataclass
import json
import logging
import time
from typing import Any

import boto3
from botocore.client import BaseClient


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


def new_requests(client: BaseClient, bucket: str) -> list[WidgetRequest]:
    """Read and delete at most one request from the request bucket."""
    if not bucket or not bucket.strip():
        raise ValueError("A request bucket name is required.")

    response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    contents = response.get("Contents", [])
    if not contents:
        return []

    key = contents[0]["Key"]
    s3_object = client.get_object(Bucket=bucket, Key=key)

    try:
        request_data = json.loads(s3_object["Body"].read())
    finally:
        s3_object["Body"].close()

    request = WidgetRequest.from_dict(request_data)

    # Remove the request only after it has been read and parsed successfully.
    client.delete_object(Bucket=bucket, Key=key)
    return [request]


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

        owner_prefix = request.owner.replace(" ", "-").lower()
        key = f"widgets/{owner_prefix}/{request.widget_id}"
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


def handle_update_request(request: WidgetRequest) -> None:
    logging.warning(
        "Update requests are not implemented yet (request ID: %s).",
        request.request_id,
    )


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
            handle_update_request(request)
        elif request.request_type == "delete":
            handle_delete_request(request)
        else:
            logging.warning(
                "Unsupported request type %r (request ID: %s).",
                request.request_type,
                request.request_id,
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consumer for widget processing"
    )
    parser.add_argument(
        "-p",
        "--profile",
        default="default",
        help="AWS profile to use (default: default)",
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
    parser.add_argument(
        "-rb",
        "--request-bucket",
        required=True,
        help="S3 bucket containing widget requests",
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
    request_s3_client = session.client("s3")

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

    start_time = time.monotonic()
    max_runtime_seconds = args.max_runtime / 1000

    print('Starting Consumer Loop...\n')
    REPORT_TIME = 5
    SLEEP_TIME = 0.1
    LOOPS = 0
    PROCESSED_REQUESTS = 0
    while True:
        if (
            max_runtime_seconds > 0
            and time.monotonic() - start_time >= max_runtime_seconds
        ):
            logging.info("Maximum runtime reached; stopping consumer.")
            break

        requests = new_requests(request_s3_client, args.request_bucket)
        if requests:
            PROCESSED_REQUESTS += 1
            logging.info("Request detected; processing.")
            process_requests(
                requests,
                widget_s3_client=widget_s3_client,
                widget_bucket=args.widget_bucket,
                dynamodb_table=dynamodb_table,
            )
        else:
            time.sleep(0.1)

        if SLEEP_TIME * LOOPS >= REPORT_TIME:
            print("REPORTING")
            print(f"Processed Requests: {PROCESSED_REQUESTS}")
            LOOPS = 0

        LOOPS += 1


if __name__ == "__main__":
    main()