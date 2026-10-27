# CloudDev Readme

Goal: to hold the code for clouddev class and have a place to keep it all.

## Python install instructions:

From the project directory, make sure Python 3.11 or newer is installed:

```bash
python3 --version
```

Create and activate a virtual environment named `clouddev`:

```bash
python3 -m venv clouddev
source clouddev/bin/activate
```

With the environment active, install the project requirements:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

To leave the environment later, run:

```bash
deactivate
```

If `python3` points to a version older than 3.11, use your system’s Python 3.11+ command instead, such as `python3.11`.

## Widget Consumer

### Run the consumer

Run from the project directory with AWS credentials configured. Make sure the venv is activated. The consumer polls the request bucket until stopped; `--max-runtime` is in milliseconds and defaults to unlimited.

```bash
python consumer.py -rb usu-cs5270-umi-requests -dwt widgets -p clouddev
```

To store widgets in Bucket 3 instead:

```bash
python consumer.py -rb usu-cs5270-umi-requests -wb usu-cs5270-umi-dist -p clouddev
```

Use `-r REGION` to select another AWS region, or `-mrt MILLISECONDS` to stop after a set time.

## Run tests

Offline tests use the local sample requests and do not call AWS:

```bash
python tests/run_unit_tests.py
```

Online tests use the `clouddev` AWS profile and the configured AWS region. They create and remove test widgets in Bucket 3 and DynamoDB. To test S3 request retrieval, provide an empty, dedicated request bucket:

```bash
AWS_PROFILE=clouddev RUN_AWS_TESTS=1 \
CONSUMER_TEST_REQUEST_BUCKET=your-empty-test-bucket \
python tests/run_unit_tests.py --suite online
```

`OK` means the selected tests passed. `skipped` means a test was disabled or its required test resource was not configured. Failures or errors need investigation. Test output is saved under `consumer-logs/unit-tests/YYYY-MM/` with an incrementing version in the filename.
