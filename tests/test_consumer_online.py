import json
import os
import unittest
import uuid

import boto3

from consumer import (
    WidgetRequest,
    handle_create_request,
    new_requests,
)


RUN_AWS_TESTS = os.environ.get("RUN_AWS_TESTS") == "1"


@unittest.skipUnless(
    RUN_AWS_TESTS,
    "Set RUN_AWS_TESTS=1 to run tests against AWS resources.",
)
class ConsumerOnlineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        profile = os.environ.get("AWS_PROFILE", "clouddev")
        region = os.environ.get(
            "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        )
        cls.session = boto3.Session(profile_name=profile, region_name=region)
        cls.s3 = cls.session.client("s3")
        cls.dynamodb = cls.session.resource("dynamodb")
        cls.widget_bucket = os.environ.get(
            "CONSUMER_TEST_WIDGET_BUCKET", "usu-cs5270-umi-dist"
        )
        cls.table_name = os.environ.get("CONSUMER_TEST_DYNAMODB_TABLE", "widgets")

    def make_request(
        self, widget_id: str, owner: str = "Unit Test Owner"
    ) -> WidgetRequest:
        return WidgetRequest.from_dict(
            {
                "type": "create",
                "requestId": f"unit-test-{uuid.uuid4()}",
                "widgetId": widget_id,
                "owner": owner,
                "label": "unit-test-label",
                "description": "created by online unit test",
                "otherAttributes": [
                    {"name": "width-unit", "value": "cm"},
                    {"name": "rating", "value": "5"},
                ],
            }
        )

    def test_create_widget_in_bucket_3_and_remove_it(self) -> None:
        widget_id = f"unit-test-{uuid.uuid4()}"
        request = self.make_request(widget_id)
        owner_prefix = request.owner.replace(" ", "-").lower()
        key = f"widgets/{owner_prefix}/{widget_id}"

        try:
            handle_create_request(
                request,
                widget_s3_client=self.s3,
                widget_bucket=self.widget_bucket,
            )
            response = self.s3.get_object(Bucket=self.widget_bucket, Key=key)
            try:
                stored_widget = json.loads(response["Body"].read())
            finally:
                response["Body"].close()

            self.assertEqual(stored_widget["widgetId"], widget_id)
            self.assertEqual(stored_widget["owner"], request.owner)
            self.assertEqual(stored_widget["label"], request.label)
            self.assertEqual(stored_widget["description"], request.description)
            self.assertEqual(
                stored_widget["otherAttributes"],
                [
                    {"name": "width-unit", "value": "cm"},
                    {"name": "rating", "value": "5"},
                ],
            )
        finally:
            self.s3.delete_object(Bucket=self.widget_bucket, Key=key)

    def test_create_widget_in_dynamodb_and_remove_it(self) -> None:
        widget_id = f"unit-test-{uuid.uuid4()}"
        request = self.make_request(widget_id)
        table = self.dynamodb.Table(self.table_name)
        table.load()
        key_schema = table.key_schema
        if key_schema != [{"AttributeName": "id", "KeyType": "HASH"}]:
            self.skipTest(
                "This test expects the widgets table to use a single id partition key; "
                f"found {key_schema!r}."
            )

        try:
            handle_create_request(request, dynamodb_table=table)
            response = table.get_item(Key={"id": widget_id}, ConsistentRead=True)
            item = response.get("Item")

            self.assertIsNotNone(item)
            self.assertEqual(item["id"], widget_id)
            self.assertEqual(item["widget_id"], widget_id)
            self.assertEqual(item["owner"], request.owner)
            self.assertEqual(item["label"], request.label)
            self.assertEqual(item["description"], request.description)
            self.assertEqual(item["width-unit"], "cm")
            self.assertEqual(item["rating"], "5")
        finally:
            table.delete_item(Key={"id": widget_id})

    def test_read_and_delete_request_from_isolated_s3_bucket(self) -> None:
        bucket = os.environ.get("CONSUMER_TEST_REQUEST_BUCKET")
        if not bucket:
            self.skipTest(
                "Set CONSUMER_TEST_REQUEST_BUCKET to an empty, dedicated test bucket."
            )

        existing = self.s3.list_objects_v2(Bucket=bucket, MaxKeys=1).get(
            "Contents", []
        )
        if existing:
            self.skipTest(
                "Request retrieval test requires an empty dedicated bucket to avoid "
                "consuming another request."
            )

        widget_id = f"unit-test-{uuid.uuid4()}"
        request_data = {
            "type": "create",
            "requestId": f"unit-test-{uuid.uuid4()}",
            "widgetId": widget_id,
            "owner": "Unit Test Owner",
        }
        key = f"unit-test-{uuid.uuid4()}.json"
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(request_data),
            ContentType="application/json",
        )

        try:
            requests = new_requests(self.s3, bucket)
            self.assertEqual(len(requests), 1)
            self.assertEqual(requests[0].widget_id, widget_id)
            remaining = self.s3.list_objects_v2(
                Bucket=bucket, Prefix=key, MaxKeys=1
            ).get("Contents", [])
            self.assertFalse(remaining)
        finally:
            self.s3.delete_object(Bucket=bucket, Key=key)


if __name__ == "__main__":
    unittest.main()
