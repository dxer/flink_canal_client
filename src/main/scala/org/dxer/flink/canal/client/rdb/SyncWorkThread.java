package org.dxer.flink.canal.client.rdb;

import com.alibaba.fastjson.JSON;
import com.zaxxer.hikari.HikariDataSource;
import org.dxer.flink.canal.client.ConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SyncWorkThread implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(SyncWorkThread.class);

    private CyclicBarrier barrier;

    private LinkedBlockingQueue<SQLRequest> bufferQueue;

    private HikariDataSource hikariDataSource;

    private final int MAX_RETRY_TIMES = 3; // 最大重试次数


    public SyncWorkThread(HikariDataSource hikariDataSource, CyclicBarrier barrier, LinkedBlockingQueue<SQLRequest> queue) {
        this.hikariDataSource = hikariDataSource;
        this.barrier = barrier;
        this.bufferQueue = queue;
    }

    @Override
    public void run() {
        SQLRequest cmd = null;
        try {
            while (true) {
                cmd = bufferQueue.poll(50, TimeUnit.MILLISECONDS);

                if (cmd != null) {
                    process(cmd);
                } else {
                    if (barrier.getNumberWaiting() > 0) {
                        barrier.await();
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("SyncWorkThread run err: {}", e);
        }
    }

    /**
     * 测试使用
     *
     * @param data
     */
    private void printProcess(SQLRequest data) {
        if (data == null) return;
        System.out.println(JSON.toJSONString(data));
    }


    private void process(SQLRequest req) {
        if (req == null) return;
        PreparedStatement pstmt = null;
        Connection connection = null;
        for (int retry = 1; retry <= MAX_RETRY_TIMES; retry++) { // 最多执行三次
            try {
                connection = hikariDataSource.getConnection();
                pstmt = connection.prepareStatement(req.getSql());
                if (ConfigConstants.ALTER.equals(req.getType())) { // 执行alter语句
                    pstmt.execute();
                } else { // 执行 insert、delete、update
                    List<Object> values = req.getValues();
                    if (values != null) {
                        for (int i = 0; i < values.size(); i++) {
                            pstmt.setObject(i + 1, values.get(i));
                        }
                    }
                    pstmt.executeUpdate();
                }
            } catch (SQLException e) {
                if (e.getMessage() != null && (e.getMessage().contains("Duplicate column name") || e.getMessage().contains("Can't DROP "))) {

                } else {
                    LOG.error("SyncWorkThread process data[{}]: {}, err: {}", retry, JSON.toJSONString(req), e); // TODO
                    if (retry >= MAX_RETRY_TIMES) {
                        throw new RuntimeException("SyncWorkThread process error.", e);
                    }
                }
            } finally {
                if (pstmt != null) {
                    try {
                        pstmt.close();
                    } catch (Exception e) {
                    }
                }

                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {
                    }
                }
            }
        }
    }
}
