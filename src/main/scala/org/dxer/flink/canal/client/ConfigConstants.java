package org.dxer.flink.canal.client;

/**
 * This class contains all constants for the configuration. That includes the configuration keys and
 * the default values.
 */
public class ConfigConstants {

    public static final String APP_NAME = "app.name";

    public static final String THREAD_NUM = "thread.num";

    public static final String PARALLELISM_NUM = "parallelism.num";

    public static final String CHECKPOINT_INTERVAL = "flink.checkpoint.interval"; // milliseconds

    // kafka source
    public static final String SOURCE_KAFKA_BOOTSTRAP_SERVERS = "source.kafka.bootstrap.servers";

    public static final String SOURCE_KAFKA_TOPICS = "source.kafka.topic";

    public static final String SOURCE_KAFKA_GROUP_ID = "source.kafka.group.id";

    // kafka sink
    public static final String SINK_PRODUCER_BOOTSTRAP_SERVERS = "sink.kafka.bootstrap.servers";

    public static final String SINK_PRODUCER_TOPIC = "sink.kafka.producer.topic";


    // rdb sink
    public static final String SINK_RDB_JDBC_URL = "sink.rdb.jdbc.url";

    public static final String SINK_RDB_JDBC_DRIVER = "sink.rdb.jdbc.driver";

    public static final String SINK_RDB_JDBC_USERNAME = "sink.rdb.jdbc.username";

    public static final String SINK_RDB_JDBC_PASSWORD = "sink.rdb.jdbc.password";


    public static final String INSERT = "INSERT";

    public static final String UPDATE = "UPDATE";

    public static final String DELETE = "DELETE";

    public static final String ALTER = "ALTER";

}
