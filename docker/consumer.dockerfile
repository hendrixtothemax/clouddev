# Runs consumer.py on Python 3.14.
#
# Build from the project root (consumer.py and requirements.txt live there):
#   docker build -f docker/consumer.dockerfile -t consumer:latest .
#
# Run with AWS credentials passed as environment variables:
#   docker run --rm --env-file docker.env consumer:latest
# Override the default arguments by adding them after the image name:
#   docker run --rm --env-file docker.env consumer:latest -rq <queue-url> -dwt widgets -mrt 60000

FROM python:3.14.10-trixie

# Print output straight away (not buffered) so `docker run` / `docker logs`
# show it live, and don't write .pyc files into the image.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first so this layer is cached when only code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

ENTRYPOINT ["python", "consumer.py"]
CMD ["-rq", "https://sqs.us-east-1.amazonaws.com/264671405801/cs5270-requests", "-dwt", "widgets"]
