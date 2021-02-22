package org.dxer.flink.canal.client.rdb;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.dxer.flink.canal.client.AppConfig;
import org.dxer.flink.canal.client.ConfigConstants;
import org.dxer.flink.canal.client.CustomKafkaDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        env.setParallelism(appConfig.getInt(ConfigConstants.PARALLELISM_NUM)); // 设置并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(appConfig.getLong(ConfigConstants.CHECKPOINT_INTERVAL)); // 设置 checkpoint 间隔时间

        FlinkKafkaConsumer<Tuple5<String, String, String, Integer, Long>> consumer = new FlinkKafkaConsumer<>(
                appConfig.getKafkaTopics(),
                new CustomKafkaDeserializationSchema(),
                appConfig.getKafkaSourceProps());

        // 设置偏移量

        DataStream<Tuple5<String, String, String, Integer, Long>> dataStream = env.addSource(consumer);
        dataStream.flatMap(new MessageFlatMapFunction(appConfig.getSyncTables()))
                .addSink(new RdbSyncSink(appConfig));

        env.execute("RdbService#" + appConfig.getAppName());

    }
}

