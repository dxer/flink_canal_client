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
public class RdbSyncService {

    private static Logger LOG = LoggerFactory.getLogger(RdbSyncService.class);

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
        env.setParallelism(1); // 设置并行度 appConfig.getInt(ConfigConstants.PARALLELISM_NUM)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(appConfig.getInt(ConfigConstants.CHECKPOINT_INTERVAL)); // 设置 checkpoint 间隔时间

        KafkaAndPartitionInfo kapInfo = appConfig.getKafkaAndPartitionInfo();

        FlinkKafkaConsumer<Tuple5<String, String, String, Integer, Long>> consumer = new FlinkKafkaConsumer<>(
                kapInfo.getTopics(),
                new CustomKafkaDeserializationSchema(),
                appConfig.getKafkaSourceProps());

        // 获取自定义的偏移量
        KafkaAndPartitionInfo info = appConfig.getKafkaAndPartitionInfo();
        // 手动设置的偏移量
        Map<KafkaTopicPartition, Long> groupOffsets = KafkaUtils.getConsumerTopicPartitionOffsets(appConfig.getKafkaSourceProps(), info.getTopics(), info.getSpecificOffsets(), info.getStartupModes());

        if (groupOffsets != null && !groupOffsets.isEmpty()) {
            consumer.setStartFromSpecificOffsets(groupOffsets);
        }

        DataStream<Tuple5<String, String, String, Integer, Long>> dataStream = env.addSource(consumer);
        dataStream.flatMap(new CanalMessageFlatMapFunction(appConfig)).name("CanalMessageFlatMapFunction")
                //.print();
                .addSink(new RdbSyncSink(appConfig)).name("RdbSyncSink");

        env.execute("RdbService#" + appConfig.getAppName());
    }
}

