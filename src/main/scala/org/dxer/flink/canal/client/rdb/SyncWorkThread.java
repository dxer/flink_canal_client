package org.dxer.flink.canal.client.rdb;

import com.alibaba.fastjson.JSON;
import com.zaxxer.hikari.HikariDataSource;
import org.dxer.flink.canal.client.ConfigConstants;
import org.dxer.flink.canal.client.entity.SQLCommand;
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

    private LinkedBlockingQueue<SQLCommand> bufferQueue;

    private HikariDataSource hikariDataSource;

    private final int MAX_RETRY_TIMES = 3; // 最大重试次数


    public SyncWorkThread(HikariDataSource hikariDataSource, CyclicBarrier barrier, LinkedBlockingQueue<SQLCommand> queue) {
        this.hikariDataSource = hikariDataSource;
        this.barrier = barrier;
        this.bufferQueue = queue;
    }

    @Override
    public void run() {
        SQLCommand cmd = null;
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
    private void printProcess(SQLCommand data) {
        if (data == null) return;
        System.out.println(JSON.toJSONString(data));
    }


    private void process(SQLCommand cmd) {
        if (cmd == null) return;
        PreparedStatement pstmt = null;
        Connection connection = null;
        for (int retry = 1; retry <= MAX_RETRY_TIMES; retry++) { // 最多执行三次
            try {
                connection = hikariDataSource.getConnection();
                pstmt = connection.prepareStatement(cmd.getSql());
                if (ConfigConstants.ALTER.equals(cmd.getType())) { // 执行alter语句
                    pstmt.execute();
                } else { // 执行 insert、delete、update
                    List<Object> values = cmd.getValues();
                    if (values != null) {
                        for (int i = 0; i < values.size(); i++) {
                            pstmt.setObject(i + 1, values.get(i));
                        }
                    }
                    pstmt.executeUpdate();
                }
            } catch (SQLException e) {
                LOG.error("SyncWorkThread process data[{}]: {}, err: {}", retry, JSON.toJSONString(cmd), e); // TODO
                if (retry >= MAX_RETRY_TIMES) {
                    throw new RuntimeException("SyncWorkThread process error.", e);
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
