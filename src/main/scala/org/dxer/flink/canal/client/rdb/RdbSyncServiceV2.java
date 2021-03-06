package org.dxer.flink.canal.client.rdb;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.dxer.flink.canal.client.AppConfig;
import org.dxer.flink.canal.client.ConfigConstants;
import org.dxer.flink.canal.client.CustomKafkaDeserializationSchema;
import org.dxer.flink.canal.client.entity.KafkaAndPartitionInfo;
import org.dxer.flink.canal.client.util.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 */
public class RdbSyncServiceV2 {

    private static Logger LOG = LoggerFactory.getLogger(RdbSyncServiceV2.class);

    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args); // 解析参数

        String filePath = tool.get("conf");

        if (filePath == null || filePath.length() <= 0) {
            System.out.println("Usage: --conf <config file path>");
            System.exit(-1);
        }

        AppConfig appConfig = new AppConfig(filePath); // 加载配置文件
        try {
            appConfig.init(); // 初始化
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (appConfig.getInt(ConfigConstants.PARALLELISM_NUM) != null) {
            env.setParallelism(appConfig.getInt(ConfigConstants.PARALLELISM_NUM)); // 设置并行度
        }
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(appConfig.getInt(ConfigConstants.CHECKPOINT_INTERVAL)); // 设置 checkpoint 间隔时间

        KafkaAndPartitionInfo kapInfo = appConfig.getKafkaAndPartitionInfo();

        // 构造 FlinkKafkaConsumer
        FlinkKafkaConsumer<Tuple5<String, String, String, Integer, Long>> consumer = new FlinkKafkaConsumer<>(
                kapInfo.getTopics(),
                new CustomKafkaDeserializationSchema(),
                appConfig.getKafkaSourceProps());

        // 获取自定义的偏移量配置
        // 获取应当设置的偏移量
        Map<KafkaTopicPartition, Long> groupOffsets = KafkaUtils.getConsumerTopicPartitionOffsets(appConfig.getKafkaSourceProps(),
                kapInfo.getTopics(), kapInfo.getSpecificOffsets(), kapInfo.getStartupModes());
        // 手动设置偏移量
        if (groupOffsets != null && !groupOffsets.isEmpty()) {
            consumer.setStartFromSpecificOffsets(groupOffsets);
        }

        DataStream<Tuple5<String, String, String, Integer, Long>> dataStream = env.addSource(consumer);
        dataStream.flatMap(new CanalMessageFlatMapFunction(appConfig)).name("CanalMessageFlatMapFunction") //.print();
                .addSink(new RdbSyncSink(appConfig)).name("RdbSyncSink");

        env.execute("RdbSyncServiceV2#" + appConfig.getAppName());
    }
}

