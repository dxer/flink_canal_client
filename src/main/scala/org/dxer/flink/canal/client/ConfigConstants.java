package org.dxer.flink.canal.client;

import java.util.ArrayList;
import java.util.List;

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

    public static final String SOURCE_KAFKA_TOPICS = "source.kafka.topics";

    public static final String SOURCE_KAFKA_TOPIC = "topic";

    public static final String SOURCE_KAFKA_GROUP_ID = "source.kafka.group.id";


    public static final String STARTUP_MODE = "startup.mode"; // 'earliest', 'latest', 'group', 'timestamp' and 'specific'
    public static final String STARTUP_SPECIFIC_OFFSETS = "startup.specific-offsets"; // partition:0,offset:42;partition:1,offset:300
    public static final String STARTUP_TIMESTAMP = "startup.timestamp-millis";

    public static final String STARTUP_MODE_EARLIEST = "earliest";
    public static final String STARTUP_MODE_LATEST = "latest";
    public static final String STARTUP_MODE_TIMESTAMP = "timestamp";
    public static final String STARTUP_MODE_SPECIFIC = "specific";


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


    public static final List<String> allowTypes = new ArrayList<>();

    static {
        allowTypes.add(INSERT);
        allowTypes.add(UPDATE);
        allowTypes.add(DELETE);
        allowTypes.add(ALTER);
    }

    public static final String SYNC_TASKS = "tasks";

    public static final String TASK_SOURCE = "source";

    public static final String TASK_TARGET = "target";

    public static final String TASK_DATABASE = "db";

    public static final String TASK_TABLE = "table";

}
