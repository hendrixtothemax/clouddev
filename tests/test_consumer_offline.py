import json
from pathlib import Path
import unittest

from consumer import (
    WidgetRequest,
    handle_create_request,
    widget_as_json_dict,
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


if __name__ == "__main__":
    unittest.main()
