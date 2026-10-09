-- Runs once when the shared Postgres container initialises (docker-entrypoint-initdb.d).
-- order_db is the container's default database; the other two services get their own, as in compose.
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
