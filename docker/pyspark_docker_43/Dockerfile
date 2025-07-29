FROM python:3.12-slim

# Install Java with more detailed repository management
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        ca-certificates \
        wget \
        gnupg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for OpenJDK 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install PySpark
RUN pip install --no-cache-dir pyspark==3.4.1 pandas setuptools pyarrow>=1.0.0

WORKDIR /app

CMD ["python"]