-- 1. Kafka Input Table
CREATE TABLE IF NOT EXISTS kafka_input
(
    raw String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'orders_stream',
    kafka_group_name = 'ch-consumer-group',
    kafka_format = 'RawBLOB',
    kafka_num_consumers = 1;

-- 2. Valid Table
CREATE TABLE IF NOT EXISTS orders_valid
(
    order_id UInt32,
    user_id UInt32,
    amount Decimal(10,2),
    ts DateTime
) ENGINE = MergeTree
ORDER BY (ts, order_id);

-- 3. DLQ Table
CREATE TABLE IF NOT EXISTS orders_dlq
(
    raw String,
    error String,
    ts DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY ts;

-- 4. Helper: Safe extract UInt
DROP FUNCTION IF EXISTS safeUInt;
CREATE FUNCTION safeUInt AS (json, key) -> 
    if(isValidJSON(json), JSONExtractUInt(json, key), 0);

-- 5. Helper: Safe extract Float
DROP FUNCTION IF EXISTS safeFloat;
CREATE FUNCTION safeFloat AS (json, key) -> 
    if(isValidJSON(json), JSONExtractFloat(json, key), 0.0);

-- 6. MV: Valid data
DROP MATERIALIZED VIEW IF EXISTS mv_valid;
CREATE MATERIALIZED VIEW mv_valid TO orders_valid AS 
SELECT JSONExtractUInt(raw, 'order_id') AS order_id, JSONExtractUInt(raw, 'user_id') AS user_id, toDecimal64(JSONExtractFloat(raw, 'amount'), 2) AS amount, toDateTime(JSONExtractUInt(raw, 'ts')) AS ts 
FROM kafka_input;


CREATE MATERIALIZED VIEW mv_dlq
TO orders_dlq
AS
SELECT
    raw,
    'Invalid JSON or missing fields' AS error,
    now() AS ts
FROM kafka_input
WHERE
    JSONExtractUInt(raw, 'order_id') = 0
    OR JSONExtractUInt(raw, 'user_id') = 0
    OR JSONExtractFloat(raw, 'amount') IS NULL
    OR JSONExtractUInt(raw, 'ts') = 0;

