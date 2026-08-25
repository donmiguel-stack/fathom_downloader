FROM python:3.12-slim

# Without this, Python block-buffers stdout when it isn't attached to a
# TTY -- which under Docker means `docker compose logs` shows nothing for
# long stretches and a perfectly healthy run looks like a hang.
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY fathom_downloader.py .
RUN pip install --no-cache-dir requests

ENTRYPOINT ["python3", "fathom_downloader.py"]
CMD ["--output-dir", "/data"]
