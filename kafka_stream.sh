eval $(cat .env)

URL="https://$SNOWFLAKE_ACCOUNT.snowflakecomputing.com"
TOPIC="LIFT_TICKETS_KAFKA_STREAMING"
NAME="LIFT_TICKETS_KAFKA_STREAMING"

curl -i -X PUT -H "Content-Type:application/json" \
    "http://localhost:8083/connectors/$NAME/config" \
    -d '{
        "connector.class":"com.snowflake.kafka.connector.SnowflakeSinkConnector",
        "errors.log.enable":"true",
        "snowflake.database.name":"BHARAT_KAFKA",
        "snowflake.private.key":"'$PRIVATE_KEY'",
        "snowflake.schema.name":"PUBLIC",
        "snowflake.role.name":"BHARAT_INGEST",
        "snowflake.url.name":"'$URL'",
        "snowflake.user.name":"'$SNOWFLAKE_USER'",
        "snowflake.enable.schematization": "TRUE",
        "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
        "topics":"'$TOPIC'",
        "name":"'$NAME'",
        "value.converter":"org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable":"false",
        "buffer.count.records":"1000000",
        "buffer.flush.time":"10",
        "buffer.size.bytes":"250000000",
        "snowflake.topic2table.map":"'$TOPIC:$NAME'"
    }'
