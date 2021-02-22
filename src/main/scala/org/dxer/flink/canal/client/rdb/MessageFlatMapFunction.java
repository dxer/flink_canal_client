package org.dxer.flink.canal.client.rdb;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.dxer.flink.canal.client.entity.Message;
import org.dxer.flink.canal.client.entity.SingleMessage;

import java.util.List;

public class MessageFlatMapFunction implements FlatMapFunction<Tuple5<String, String, String, Integer, Long>, SingleMessage> {

    private List<String> syncTables = null;

    public MessageFlatMapFunction(List<String> tables) {
        this.syncTables = tables;
    }

    @Override
    public void flatMap(Tuple5<String, String, String, Integer, Long> value, final Collector<SingleMessage> out) throws Exception {
        // 过滤需要的表，过滤需要的
        Message message = JSON.parseObject(value.f1, Message.class);
        if (message != null) {
            String table = message.getTable();
            if (syncTables != null && syncTables.contains(table.trim().toString())) {
                List<SingleMessage> singleMessages = SingleMessage.message2SingleMessages(message);
                singleMessages.forEach(x -> {
                    x.setTopic(value.f2); // 设置主题
                    x.setOffset(value.f4); // 设置偏移量
                    out.collect(x); // 数据继续往下传递
                });
            }
        }
    }
}
