FROM python:3.9-slim

WORKDIR /app

# Install dbt and dependencies
RUN pip install --no-cache-dir dbt-postgres dbt-core psycopg2-binary

# Add wait script for service dependencies
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.9.0/wait /wait
RUN chmod +x /wait

# Keep container running
CMD ["tail", "-f", "/dev/null"]