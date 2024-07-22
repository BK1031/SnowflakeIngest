eval $(cat .env)

URL="https://$SNOWFLAKE_ACCOUNT.snowflakecomputing.com"
NAME="LIFT_TICKETS_KAFKA_BATCH"

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
        "topics":"'$KAFKA_TOPIC'",
        "name":"'$NAME'",
        "buffer.size.bytes":"250000000",
        "buffer.flush.time":"60",
        "buffer.count.records":"1000000",
        "snowflake.topic2table.map":"'$KAFKA_TOPIC:$NAME'"
    }'