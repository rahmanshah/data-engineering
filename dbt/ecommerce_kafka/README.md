# Build and start the containers
docker-compose up -d

# Create schemas
docker-compose exec dbt python -c "
import psycopg2
conn = psycopg2.connect(host='postgres', dbname='ecommerce_kafka', user='dbt_user', password='dbt_password')
conn.autocommit = True
cursor = conn.cursor()
for schema in ['raw_data', 'staging', 'analytics']:
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
cursor.close()
conn.close()
```markdown
## Build and start the containers

```sh
docker-compose up -d
```

## Create schemas

```sh
docker-compose exec dbt python -c "
import psycopg2
conn = psycopg2.connect(host='postgres', dbname='ecommerce_kafka', user='dbt_user', password='dbt_password')
conn.autocommit = True
cursor = conn.cursor()
for schema in ['raw_data', 'staging', 'analytics']:
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
cursor.close()
conn.close()
"
```

## Produce data to Kafka

```sh
docker-compose exec kafka-producer python scripts/produce_customer_data.py
docker-compose exec kafka-producer python scripts/produce_order_data.py
```

## Consume data from Kafka to PostgreSQL

```sh
docker-compose exec kafka-consumer python scripts/consume_to_postgres.py
```

## Install dbt dependencies

```sh
docker-compose exec dbt dbt deps
```

## Run dbt models

```sh
docker-compose exec dbt dbt run
```

## Test dbt models

```sh
docker-compose exec dbt dbt test
```

## Generate documentation

```sh
docker-compose exec dbt dbt docs generate
docker-compose exec dbt dbt docs serve
```
## Produce data to Kafka

```sh
docker-compose exec kafka-producer python scripts/produce_customer_data.py
docker-compose exec kafka-producer python scripts/produce_order_data.py
```

## Consume data from Kafka to PostgreSQL

```sh
docker-compose exec kafka-consumer python scripts/consume_to_postgres.py
```

## Install dbt dependencies

```sh
docker-compose exec dbt dbt deps
```

## Run dbt models

```sh
docker-compose exec dbt dbt run
```

## Test dbt models

```sh
docker-compose exec dbt dbt test
```

## Generate documentation

```sh
docker-compose exec dbt dbt docs generate
docker-compose exec dbt dbt docs serve
```

# Produce data to Kafka
docker-compose exec kafka-producer python scripts/produce_customer_data.py
docker-compose exec kafka-producer python scripts/produce_order_data.py

# Consume data from Kafka to PostgreSQL
docker-compose exec kafka-consumer python scripts/consume_to_postgres.py

# Install dbt dependencies
docker-compose exec dbt dbt deps

# Run dbt models
docker-compose exec dbt dbt run

# Test dbt models
docker-compose exec dbt dbt test

# Generate documentation
docker-compose exec dbt dbt docs generate
docker-compose exec dbt dbt docs serve