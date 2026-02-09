CREATE SOURCE clickstream_kafka
FROM KAFKA BROKER 'kafka:9092' TOPIC 'clicks'
FORMAT JSON;

CREATE MATERIALIZED VIEW clicks_count AS
SELECT user_id, SUM(clicks) AS total_clicks
FROM clickstream_kafka
GROUP BY user_id;

SELECT * FROM clicks_count;
