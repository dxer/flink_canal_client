package org.dxer.flink.canal.client.rdb;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.dxer.flink.canal.client.AppConfig;
import org.dxer.flink.canal.client.entity.Message;
import org.dxer.flink.canal.client.entity.SingleMessage;
import org.dxer.flink.canal.client.entity.TaskInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CanalMessageFlatMapFunction implements FlatMapFunction<Tuple5<String, String, String, Integer, Long>, SingleMessage> {

    private Map<String, String> dbMapping = null;

    public CanalMessageFlatMapFunction(AppConfig appConfig) {
        this.dbMapping = appConfig.getDBMappings();
    }

    @Override
    public void flatMap(Tuple5<String, String, String, Integer, Long> value, final Collector<SingleMessage> out) throws Exception {
        // 过滤需要的表，过滤需要的
        Message message = JSON.parseObject(value.f1, Message.class);
        if (message != null) {
            String database = message.getDatabase();
            String table = message.getTable();
            String fullTableName = database.trim() + "." + table.trim();
            if (dbMapping.containsKey(fullTableName)) {  // 进行过滤，选择配置了的表
                List<SingleMessage> singleMessages = SingleMessage.message2SingleMessages(message);
                singleMessages.forEach(x -> {
                    x.setTopic(value.f2); // 设置主题
                    x.setOffset(value.f4); // 设置偏移量
                    x.setPartitionId(value.f3); // 设置分区id
                    out.collect(x); // 数据继续往下传递
                });
            }
        }
    }
}
