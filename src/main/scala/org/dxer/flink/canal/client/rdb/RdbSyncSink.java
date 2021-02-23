package org.dxer.flink.canal.client.rdb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.dxer.flink.canal.client.AppConfig;
import org.dxer.flink.canal.client.ConfigConstants;
import org.dxer.flink.canal.client.entity.RowData;
import org.dxer.flink.canal.client.entity.SingleMessage;
import org.dxer.flink.canal.client.util.SqlHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RdbSyncSink extends RichSinkFunction<SingleMessage> implements CheckpointedFunction {

    private static Logger LOG = LoggerFactory.getLogger(RdbSyncSink.class);


    private CyclicBarrier cyclicBarrier;
    private AppConfig appConfig;

    private int threadNum = 1;

    private List<LinkedBlockingQueue<RowData>> queues = new ArrayList<>();
    private static final int DEFAULT_QUEUE_CAPACITY = 1000;


    public RdbSyncSink(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    private HikariDataSource newHikariDataSource(String url, String driver, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setDriverClassName(driver);
        config.setUsername(username);
        config.setPassword(password);
        config.setIdleTimeout(60000);
        config.setValidationTimeout(3000);
        config.setMaxLifetime(60000);
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(10);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        return new HikariDataSource(config);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.threadNum = appConfig.getDBMappings().size() >= 5 ? 5 : appConfig.getDBMappings().size();
        this.cyclicBarrier = new CyclicBarrier(this.threadNum + 1);

        String url = appConfig.getString(ConfigConstants.SINK_RDB_JDBC_URL);
        String driver = appConfig.getString(ConfigConstants.SINK_RDB_JDBC_DRIVER);
        String username = appConfig.getString(ConfigConstants.SINK_RDB_JDBC_USERNAME);
        String password = appConfig.getString(ConfigConstants.SINK_RDB_JDBC_PASSWORD);


        HikariDataSource hikariDataSource = newHikariDataSource(url, driver, username, password);

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(this.threadNum, this.threadNum, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        for (int i = 0; i < threadNum; i++) { // 新建线程
            LinkedBlockingQueue<RowData> queue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_CAPACITY); // 一个线程一个队列
            threadPoolExecutor.execute(new SyncWorkThread(hikariDataSource, cyclicBarrier, queue));
            queues.add(queue);
        }
        LOG.info("There are {} threads created.", this.threadNum);


    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        cyclicBarrier.await();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void invoke(SingleMessage message, Context context) throws Exception {
        String fullTableName = message.getDatabase() + "." + message.getTable();
        RowData metaData = SqlHelper.buildSQL(message, appConfig.getDBMappings().get(fullTableName));

        int index = metaData.getTable().hashCode() % queues.size();
        LinkedBlockingQueue<RowData> queue = queues.get(index);

        if (queue != null) {
            queue.put(metaData);
        } else {
            LOG.error("{}#{}#{} not match any queue", metaData.getTable(), metaData.getSql(), metaData.getValues());
        }
    }
}
