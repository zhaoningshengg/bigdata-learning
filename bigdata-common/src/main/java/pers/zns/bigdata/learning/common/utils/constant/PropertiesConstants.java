package pers.zns.bigdata.learning.common.utils.constant;

/**
 * @program: bigdata-learning
 * @description:
 * @author: zns
 * @create: 2022-05-30 15:26
 */
public class PropertiesConstants {
    public static final String ACTIVE = "active";
    public static final String DEFAULT_ACTIVE = "/wx.properties";

    public static final String JOB_NAME = "pipeline.name";
    public static final String ZZZ = "ZZZ";
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "test";
    public static final String METRICS_TOPIC = "metrics.topic";
    public static final String SOURCE_TOPIC = "kafka.source.topic";
    public static final String SINK_TOPIC = "kafka.sink.topic";
    public static final String CONSUMER_FROM_TIME = "consumer.from.time";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    public static final String CHECKPOINT_MEMORY = "memory";
    public static final String CHECKPOINT_FS = "fs";
    public static final String CHECKPOINT_ROCKETSDB = "rocksdb";

    //es config
    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions";
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";

    //mysql
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String MYSQL_TABLE = "mysql.table";
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";


    public static final String SQL_CATALOG_PATH = "sql.catalog.path";
    public static final String SQL_CATALOG_PATH_DEFAULT = "sql.catalog.path.default";

    public static final String SQL_FILES_KEY = "sql.file.paths";
    public static final String SQL_STRINGS_KEY = "sql.strings";

    public static final String MONITOR_SQL_KEY = "monitor.sql";

    public static final String BATCH_INTERVAL_MINUTES = "batch.interval.minutes";

}