import json
from pathlib import Path
import unittest

from botocore.exceptions import ClientError

from consumer import (
    WidgetRequest,
    build_dynamodb_update,
    handle_create_request,
    handle_update_request,
    merge_widget_update,
    widget_as_json_dict,
    widget_s3_key,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_REQUESTS = PROJECT_ROOT / "sample-requests" / "sample-requests"
EMPTY_SAMPLE = "1612306374392"
INVALID_TYPE_SAMPLE = "1612306375135"
MISSING_OWNER_SAMPLE = "1612306375892"
INVALID_SAMPLE_NAMES = {
    EMPTY_SAMPLE,
    INVALID_TYPE_SAMPLE,
    MISSING_OWNER_SAMPLE,
}


class WidgetRequestParsingTests(unittest.TestCase):
    def test_local_sample_requests_parse(self) -> None:
        sample_paths = sorted(
            path
            for path in SAMPLE_REQUESTS.iterdir()
            if path.is_file()
            and not path.name.startswith((".", "__"))
            and path.name not in INVALID_SAMPLE_NAMES
        )
        self.assertTrue(sample_paths, f"No samples found in {SAMPLE_REQUESTS}")

        for path in sample_paths:
            with self.subTest(sample=path.name):
                data = json.loads(path.read_text(encoding="utf-8"))
                request = WidgetRequest.from_dict(data)

                self.assertIn(request.request_type, {"create", "update", "delete"})
                self.assertTrue(request.request_id)
                self.assertTrue(request.widget_id)
                self.assertTrue(request.owner)
                self.assertEqual(
                    len(request.other_attributes or []),
                    len(data.get("otherAttributes", [])),
                )

    def test_invalid_local_samples_are_rejected(self) -> None:
        empty_sample = SAMPLE_REQUESTS / EMPTY_SAMPLE
        with self.subTest(sample=EMPTY_SAMPLE):
            with self.assertRaises(json.JSONDecodeError):
                json.loads(empty_sample.read_text(encoding="utf-8"))

        invalid_type = json.loads(
            (SAMPLE_REQUESTS / INVALID_TYPE_SAMPLE).read_text(encoding="utf-8")
        )
        with self.subTest(sample=INVALID_TYPE_SAMPLE):
            with self.assertRaisesRegex(ValueError, "Unsupported request type"):
                WidgetRequest.from_dict(invalid_type)

        missing_owner = json.loads(
            (SAMPLE_REQUESTS / MISSING_OWNER_SAMPLE).read_text(encoding="utf-8")
        )
        with self.subTest(sample=MISSING_OWNER_SAMPLE):
            with self.assertRaises(KeyError):
                WidgetRequest.from_dict(missing_owner)

    def test_parse_optional_fields_and_attributes(self) -> None:
        request = WidgetRequest.from_dict(
            {
                "type": "CREATE",
                "requestId": "request-1",
                "widgetId": "widget-1",
                "owner": "Mary Matthews",
                "label": "sample",
                "description": "description",
                "otherAttributes": [{"name": "width-unit", "value": "cm"}],
            }
        )

        self.assertEqual(request.request_type, "create")
        self.assertEqual(request.label, "sample")
        self.assertEqual(request.description, "description")
        self.assertEqual(request.other_attributes[0].name, "width-unit")
        self.assertEqual(request.other_attributes[0].value, "cm")

    def test_missing_optional_fields_default_to_none(self) -> None:
        request = WidgetRequest.from_dict(
            {
                "type": "create",
                "requestId": "request-1",
                "widgetId": "widget-1",
                "owner": "Mary Matthews",
            }
        )

        self.assertIsNone(request.label)
        self.assertIsNone(request.description)
        self.assertIsNone(request.other_attributes)

    def test_unsupported_request_type_raises_value_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported request type"):
            WidgetRequest.from_dict(
                {
                    "type": "archive",
                    "requestId": "request-1",
                    "widgetId": "widget-1",
                    "owner": "Mary Matthews",
                }
            )

    def test_missing_required_field_raises_key_error(self) -> None:
        with self.assertRaises(KeyError):
            WidgetRequest.from_dict(
                {
                    "type": "create",
                    "requestId": "request-1",
                    "owner": "Mary Matthews",
                }
            )


class WidgetSerializationTests(unittest.TestCase):
    def test_serializes_widget_fields_without_request_metadata(self) -> None:
        sample_path = next(
            path
            for path in sorted(SAMPLE_REQUESTS.iterdir())
            if path.is_file()
            and not path.name.startswith((".", "__"))
            and path.name not in INVALID_SAMPLE_NAMES
        )
        request = WidgetRequest.from_dict(
            json.loads(sample_path.read_text(encoding="utf-8"))
        )

        widget = widget_as_json_dict(request)

        self.assertEqual(widget["widgetId"], request.widget_id)
        self.assertEqual(widget["owner"], request.owner)
        self.assertNotIn("requestId", widget)
        self.assertNotIn("type", widget)
        if request.label is not None:
            self.assertEqual(widget["label"], request.label)
        if request.description is not None:
            self.assertEqual(widget["description"], request.description)
        if request.other_attributes:
            self.assertEqual(
                widget["otherAttributes"],
                [
                    {"name": attribute.name, "value": attribute.value}
                    for attribute in request.other_attributes
                ],
            )


class CreateRequestValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.request = WidgetRequest.from_dict(
            {
                "type": "create",
                "requestId": "request-1",
                "widgetId": "widget-1",
                "owner": "Mary Matthews",
            }
        )

    def test_missing_storage_destination_raises_value_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "No widget storage destination"):
            handle_create_request(self.request)

    def test_s3_destination_without_client_raises_value_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "S3 client is required"):
            handle_create_request(self.request, widget_bucket="test-bucket")


def make_update(**fields) -> WidgetRequest:
    data = {
        "type": "update",
        "requestId": "update-1",
        "widgetId": "widget-1",
        "owner": "Mary Matthews",
    }
    data.update(fields)
    return WidgetRequest.from_dict(data)


def client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class FakeBody:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self) -> bytes:
        return self.data

    def close(self) -> None:
        pass


class FakeS3:
    """Stores objects in a dict; get_object on a missing key raises NoSuchKey."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket, Key, Body, ContentType=None) -> None:
        self.objects[(Bucket, Key)] = Body.encode() if isinstance(Body, str) else Body

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise client_error("NoSuchKey", "GetObject")
        return {"Body": FakeBody(self.objects[(Bucket, Key)])}

    def stored(self, bucket: str, key: str) -> dict:
        return json.loads(self.objects[(bucket, key)])


class FakeTable:
    """Records update_item calls; can pretend the widget does not exist."""

    def __init__(self, widget_exists: bool = True) -> None:
        self.widget_exists = widget_exists
        self.calls: list[dict] = []

    def update_item(self, **kwargs) -> None:
        self.calls.append(kwargs)
        if not self.widget_exists:
            raise client_error("ConditionalCheckFailedException", "UpdateItem")


class MergeWidgetUpdateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.widget = {
            "widgetId": "widget-1",
            "owner": "Mary Matthews",
            "label": "old label",
            "description": "old description",
            "otherAttributes": [
                {"name": "color", "value": "blue"},
                {"name": "size", "value": "10"},
            ],
        }

    def test_replaces_label_and_description(self) -> None:
        updated = merge_widget_update(
            self.widget, make_update(label="new", description="newer")
        )
        self.assertEqual(updated["label"], "new")
        self.assertEqual(updated["description"], "newer")

    def test_leaves_fields_missing_from_request_unchanged(self) -> None:
        updated = merge_widget_update(self.widget, make_update())
        self.assertEqual(updated["label"], "old label")
        self.assertEqual(updated["description"], "old description")
        self.assertEqual(updated["otherAttributes"], self.widget["otherAttributes"])

    def test_adds_replaces_and_removes_attributes(self) -> None:
        request = make_update(
            otherAttributes=[
                {"name": "color", "value": "red"},
                {"name": "size", "value": ""},
                {"name": "vendor", "value": "ACME"},
            ]
        )
        updated = merge_widget_update(self.widget, request)
        self.assertEqual(
            updated["otherAttributes"],
            [
                {"name": "color", "value": "red"},
                {"name": "vendor", "value": "ACME"},
            ],
        )

    def test_removing_last_attribute_drops_the_list(self) -> None:
        widget = {**self.widget, "otherAttributes": [{"name": "color", "value": "blue"}]}
        request = make_update(otherAttributes=[{"name": "color", "value": ""}])
        self.assertNotIn("otherAttributes", merge_widget_update(widget, request))

    def test_does_not_modify_the_original(self) -> None:
        original = json.dumps(self.widget, sort_keys=True)
        merge_widget_update(self.widget, make_update(label="new"))
        self.assertEqual(json.dumps(self.widget, sort_keys=True), original)


class BuildDynamoDbUpdateTests(unittest.TestCase):
    def resolve(self, args: dict) -> tuple[dict, list[str]]:
        """Turn the placeholder expression back into {name: value} and removals."""
        names = args["ExpressionAttributeNames"]
        values = args["ExpressionAttributeValues"]
        expression = args["UpdateExpression"]
        set_part, _, remove_part = expression.partition(" REMOVE ")
        sets = {}
        for assignment in set_part.removeprefix("SET ").split(", "):
            name, value = assignment.split(" = ")
            sets[names[name]] = values[value]
        removals = [names[n] for n in remove_part.split(", ")] if remove_part else []
        return sets, removals

    def test_sets_fields_and_attributes(self) -> None:
        args = build_dynamodb_update(
            make_update(
                label="new",
                otherAttributes=[{"name": "width-unit", "value": "cm"}],
            )
        )
        sets, removals = self.resolve(args)
        self.assertEqual(
            sets, {"owner": "Mary Matthews", "label": "new", "width-unit": "cm"}
        )
        self.assertEqual(removals, [])

    def test_empty_attribute_value_is_removed(self) -> None:
        args = build_dynamodb_update(
            make_update(otherAttributes=[{"name": "color", "value": ""}])
        )
        sets, removals = self.resolve(args)
        self.assertNotIn("color", sets)
        self.assertEqual(removals, ["color"])

    def test_requires_widget_to_exist(self) -> None:
        args = build_dynamodb_update(make_update())
        self.assertEqual(args["ConditionExpression"], "attribute_exists(#id)")
        self.assertEqual(args["ExpressionAttributeNames"]["#id"], "id")

    def test_key_attributes_are_never_changed(self) -> None:
        args = build_dynamodb_update(
            make_update(
                otherAttributes=[
                    {"name": "id", "value": "hijack"},
                    {"name": "widget_id", "value": ""},
                ]
            )
        )
        sets, removals = self.resolve(args)
        self.assertNotIn("id", sets)
        self.assertNotIn("widget_id", removals)


class HandleUpdateRequestTests(unittest.TestCase):
    def test_s3_update_merges_into_stored_widget(self) -> None:
        s3 = FakeS3()
        create = WidgetRequest.from_dict(
            {
                "type": "create",
                "requestId": "create-1",
                "widgetId": "widget-1",
                "owner": "Mary Matthews",
                "label": "old",
                "otherAttributes": [{"name": "color", "value": "blue"}],
            }
        )
        handle_create_request(create, widget_s3_client=s3, widget_bucket="b")

        updated = handle_update_request(
            make_update(
                label="new",
                otherAttributes=[{"name": "size", "value": "5"}],
            ),
            widget_s3_client=s3,
            widget_bucket="b",
        )

        self.assertTrue(updated)
        stored = s3.stored("b", widget_s3_key(create))
        self.assertEqual(stored["label"], "new")
        self.assertEqual(
            stored["otherAttributes"],
            [{"name": "color", "value": "blue"}, {"name": "size", "value": "5"}],
        )

    def test_s3_update_of_missing_widget_returns_false(self) -> None:
        s3 = FakeS3()
        with self.assertLogs(level="WARNING") as logs:
            updated = handle_update_request(
                make_update(label="new"), widget_s3_client=s3, widget_bucket="b"
            )
        self.assertFalse(updated)
        self.assertEqual(s3.objects, {})  # nothing was created
        self.assertIn("Update failed", logs.output[0])

    def test_dynamodb_update_uses_widget_id_as_key(self) -> None:
        table = FakeTable()
        self.assertTrue(handle_update_request(make_update(), dynamodb_table=table))
        self.assertEqual(table.calls[0]["Key"], {"id": "widget-1"})

    def test_dynamodb_update_of_missing_widget_returns_false(self) -> None:
        table = FakeTable(widget_exists=False)
        with self.assertLogs(level="WARNING") as logs:
            updated = handle_update_request(make_update(), dynamodb_table=table)
        self.assertFalse(updated)
        self.assertIn("Update failed", logs.output[0])

    def test_other_dynamodb_errors_are_raised(self) -> None:
        class BrokenTable:
            def update_item(self, **kwargs):
                raise client_error("ProvisionedThroughputExceededException", "UpdateItem")

        with self.assertRaises(ClientError):
            handle_update_request(make_update(), dynamodb_table=BrokenTable())

    def test_missing_storage_destination_raises_value_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "No widget storage destination"):
            handle_update_request(make_update())


if __name__ == "__main__":
    unittest.main()
