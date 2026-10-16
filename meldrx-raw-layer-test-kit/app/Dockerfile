FROM python:3.11-slim
RUN pip install --no-cache-dir boto3==1.40.0
WORKDIR /app
COPY meldrx_ingest.py post_check.py run_job.sh /app/
RUN chmod +x /app/run_job.sh
ENTRYPOINT ["/app/run_job.sh"]
# Real setup: CMD ["poetry", "run", "dbt", "build"]. Test setup: Athena check instead of dbt.
CMD ["python", "/app/post_check.py"]
