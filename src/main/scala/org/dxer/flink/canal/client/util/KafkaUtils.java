package org.dxer.flink.canal.client.util;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * kafka 工具类
 */
public class KafkaUtils {

    private static Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

    /**
     * 解析字符串获取主题对应分区的指定的偏移量
     *
     * @param s
     * @return
     */
    private static Map<Integer, Long> parseSpecificOffsets(String s) {
        Map<Integer, Long> specificOffsets = null;
        if (!Strings.isNullOrEmpty(s)) {
            specificOffsets = new HashMap<>();
            String[] partitionOffsetStr = s.split(",");
            if (partitionOffsetStr != null && partitionOffsetStr.length > 0) {
                for (String pos : partitionOffsetStr) {
                    String[] parts = pos.split(":");
                    if (parts == null || parts.length != 2) continue;
                    specificOffsets.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
                }
            }
        }
        return specificOffsets;
    }

    /**
     * @param props
     * @param topics
     * @return
     */
    public static Map<TopicPartition, Long> getPartitionEndOffsets(Properties props, List<String> topics) {
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", props.getProperty("bootstrap.servers"));
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer(properties);
        try {
            for (String topic : topics) {
                List<PartitionInfo> partitionsInfo = consumer.partitionsFor(topic);
                Set<TopicPartition> topicPartitions = new HashSet<>();
                for (PartitionInfo partitionInfo : partitionsInfo) {
                    TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                    topicPartitions.add(topicPartition);
                }
                Map map = consumer.endOffsets(topicPartitions);
                if (map != null && map.size() > 0) {
                    endOffsets.putAll(map);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            consumer.close();
        }
        return endOffsets;
    }

    /**
     * @param props
     * @param topics
     * @param specificOffsets
     * @param startupModes
     * @return
     */
    public static Map<KafkaTopicPartition, Long> getConsumerTopicPartitionOffsets(Properties props, List<String> topics, Map<String, String> specificOffsets, Map<String, String> startupModes) {
        Map<KafkaTopicPartition, Long> groupOffsets = null;
        if ((startupModes != null && !startupModes.isEmpty()) || (specificOffsets != null && !specificOffsets.isEmpty())) {
            // 获取所有主题的最新的偏移量即可
            groupOffsets = new HashMap<>();
            Map<TopicPartition, Long> endOffsets = getPartitionEndOffsets(props, topics); // 获取每个 partition 的最大的偏移量

            if (endOffsets != null) {
                for (TopicPartition tp : endOffsets.keySet()) {
                    System.out.println(tp.topic() + ", " + tp.partition() + ", max-offsets: " + endOffsets.get(tp)); // 打印最大的偏移量
                }
            }

            if (startupModes != null && !startupModes.isEmpty()) {
                Map<String, TopicDescription> topicPartitions = getTopicPartitions(props, new ArrayList<>(startupModes.keySet())); // 获取所有主题信息

                for (String topic : startupModes.keySet()) {
                    TopicDescription topicDesc = topicPartitions.get(topic);
                    String startupMode = startupModes.get(topic);
                    List<TopicPartitionInfo> partitions = topicDesc.partitions();
                    if (startupMode.equalsIgnoreCase("earliest")) { // 最早位置消费，这里将偏移量设置为0，如果当前分区的最小偏移量大于0，client会自动将设置偏移量设置为最小的偏移量
                        for (TopicPartitionInfo tpi : partitions) {
                            groupOffsets.put(new KafkaTopicPartition(topic, tpi.partition()), 0L);
                        }
                    } else if (startupMode.equalsIgnoreCase("latest")) { // 将偏移量设置为最大的偏移量
                        for (TopicPartitionInfo tpi : partitions) {
                            Long endOffset = endOffsets.get(new TopicPartition(topic, tpi.partition()));
                            groupOffsets.put(new KafkaTopicPartition(topic, tpi.partition()), endOffset);
                        }
                    }
                }
            }

            // 设置了具体的分区偏移量，解析配置，并设置偏移量
            if (specificOffsets != null && !specificOffsets.isEmpty()) {
                for (String topic : specificOffsets.keySet()) {
                    String specificOffsetsStr = specificOffsets.get(topic);
                    Map<Integer, Long> specificOffsetsMap = parseSpecificOffsets(specificOffsetsStr); // 获取指定的偏移量
                    if (specificOffsets != null && specificOffsetsMap != null) {
                        for (int partition : specificOffsetsMap.keySet()) {
                            Long offset = specificOffsetsMap.get(partition); // 需要设置的偏移量
                            TopicPartition tp = new TopicPartition(topic, partition);
                            Long endOffset = endOffsets.get(tp); // 获取该分区的最大偏移量
                            if (endOffset != null) {
                                if (offset > endOffset) { // 如果需要设置的偏移量大于分区最大偏移量，则将偏移量设置为最大偏移量
                                    groupOffsets.put(new KafkaTopicPartition(topic, partition), endOffset);
                                } else { // 如果需要设置的偏移量小于或等于该分区的最大偏移量，则将偏移量设置为该指定的偏移量
                                    groupOffsets.put(new KafkaTopicPartition(topic, partition), offset);
                                }
                            } else {
                                groupOffsets.put(new KafkaTopicPartition(topic, partition), 0L);
                            }
                        }
                    }
                }
            }
        }
        return groupOffsets;
    }


    /**
     * 获取kafka偏移量
     *
     * @param props
     * @param topics
     * @param group
     * @param specificOffsets
     * @return
     * @throws Exception
     */
    public static Map<KafkaTopicPartition, Long> getKafkaPartitionOffsets(Properties props, List<String> topics, String group, Map<String, String> specificOffsets, Map<String, String> startupModes) throws Exception {
        Map<KafkaTopicPartition, Long> groupOffsets = new HashMap<>();

        // 先从kafka中获取指定路径的偏移量
        Map<String, Map<KafkaTopicPartition, Long>> topicGroupOffsets = getTopicGroupOffsets(props, topics, group);
        // 由配置文件中单独配置的每个主题的偏移量
        if (specificOffsets != null && !specificOffsets.isEmpty()) {
            for (String topic : specificOffsets.keySet()) {
                String value = specificOffsets.get(topic);
                String[] partitionOffsets = value.split(",");

                Map<KafkaTopicPartition, Long> topicPartitionLongMap = new HashMap<>();
                for (String po : partitionOffsets) {
                    String[] parts = po.split(":");
                    if (parts != null && parts.length == 2) {
                        KafkaTopicPartition ktp = new KafkaTopicPartition(topic, Integer.parseInt(parts[0]));
                        topicPartitionLongMap.put(ktp, Long.parseLong(parts[1]));
                    }
                }
                topicGroupOffsets.put(topic, topicPartitionLongMap); // 覆盖以前的记录，使用自定义的偏移量
            }
        }

        if (startupModes != null && !startupModes.isEmpty()) {
            for (String topic : startupModes.keySet()) {
                String startupMode = startupModes.get(topic).toLowerCase();
                Map<KafkaTopicPartition, Long> topicPartitionLongMap = topicGroupOffsets.get(topic);
                Iterator<Map.Entry<KafkaTopicPartition, Long>> iterator = topicPartitionLongMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<KafkaTopicPartition, Long> entry = iterator.next();
                    if (startupMode.equalsIgnoreCase("earliest-offset")) {
                        entry.setValue(KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET);
                    } else if (startupMode.equalsIgnoreCase("latest-offset")) {
                        entry.setValue(KafkaTopicPartitionStateSentinel.LATEST_OFFSET);
                    }
                }
            }
        }

        if (topicGroupOffsets != null) {
            for (String topic : topicGroupOffsets.keySet()) {
                Map<KafkaTopicPartition, Long> topicPartitionLongMap = topicGroupOffsets.get(topic);
                groupOffsets.putAll(topicPartitionLongMap);
            }
        }

        // 打印分区偏移量信息
        if (groupOffsets != null && !groupOffsets.isEmpty()) {
            for (KafkaTopicPartition ktp : groupOffsets.keySet()) {
                System.out.println(" ==> " + ktp.getTopic() + ", " + ktp.getPartition() + ", " + groupOffsets.get(ktp));
            }
        } else {
            System.out.println("Not Found group offsets.");
        }

        return groupOffsets;
    }

    /**
     * 获取所有 topic partition 信息
     *
     * @param props
     * @param topics
     * @return
     */
    public static Map<String, TopicDescription> getTopicPartitions(Properties props, List<String> topics) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", props.getProperty("bootstrap.servers"));
        AdminClient adminClient = KafkaAdminClient.create(kafkaProps);

        Map<String, TopicDescription> topicDescMap = new HashMap<>();

        try {
            Map<String, KafkaFuture<TopicDescription>> values = adminClient.describeTopics(topics).values();
            for (String topic : values.keySet()) {
                TopicDescription topicDesc = values.get(topic).get();
                topicDescMap.put(topic, topicDesc);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }

        return topicDescMap;
    }


    /**
     * 获取指定topic-group的偏移量信息
     *
     * @param props
     * @param topics topic 集合
     * @param group  消费者组
     * @return
     * @throws Exception
     */
    public static Map<String, Map<KafkaTopicPartition, Long>> getTopicGroupOffsets(Properties props, List<String> topics, String group) throws Exception {
        Map<String, Map<KafkaTopicPartition, Long>> topicPartitions = new HashMap<>();
        if (topics == null || topics.size() == 0) {
            return topicPartitions;
        }

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", props.getProperty("bootstrap.servers"));
        AdminClient adminClient = KafkaAdminClient.create(kafkaProps);


        // 获取所有topic的partition
        Map<String, TopicDescription> topicDescriptionMap = adminClient.describeTopics(topics).all().get();

        Map<KafkaTopicPartition, TopicPartitionInfo> allTopicPartitionMap = new HashMap<>();
        for (String topic : topicDescriptionMap.keySet()) {
            TopicDescription topicDescription = topicDescriptionMap.get(topic);
            List<TopicPartitionInfo> partitions = topicDescription.partitions();
            for (TopicPartitionInfo tpi : partitions) {
                allTopicPartitionMap.put(new KafkaTopicPartition(topic, tpi.partition()), tpi);
            }
        }

        // 从 kafka 中获取 group 的消费的偏移量
        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(group);
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get(); // 获取group消费的偏移量

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetAndMetadataMap.entrySet()) {
            TopicPartition topicpartition = entry.getKey(); // 某一個topic的某一個partition
            String topic = topicpartition.topic();
            if (topics.contains(topic)) { // 是要获取的topic
                OffsetAndMetadata offset = entry.getValue(); // offset

                Map<KafkaTopicPartition, Long> topicPartitionLongMap = topicPartitions.get(topic);
                if (topicPartitionLongMap == null) {
                    topicPartitionLongMap = new HashMap<>();
                }
                topicPartitionLongMap.put(new KafkaTopicPartition(topicpartition.topic(), topicpartition.partition()), offset.offset());
                topicPartitions.put(topic, topicPartitionLongMap);
            }
        }

        // 补充没有group消费记录，从最早开始消费
        for (KafkaTopicPartition ktp : allTopicPartitionMap.keySet()) {
            String topic = ktp.getTopic(); // topic
            Map<KafkaTopicPartition, Long> topicPartitionLongMap = topicPartitions.get(topic);
            if (topicPartitionLongMap != null) {
                if (!topicPartitionLongMap.containsKey(ktp)) {
                    topicPartitionLongMap.put(ktp, 0L);
                }
            } else { // 没有获取到 group 的偏移量，设置从earliest offset 开始消费
                topicPartitionLongMap = new HashMap<>();
                topicPartitionLongMap.put(ktp, 0L);
                topicPartitions.put(topic, topicPartitionLongMap);
            }

        }

        if (adminClient != null) {
            adminClient.close();
        }
        return topicPartitions;
    }

    /**
     * 获取所有topic-group的偏移量信息
     *
     * @param props
     * @return
     * @throws Exception
     */
    public static Map<KafkaTopicPartition, Long> getAllGroupOffsets(Properties props) throws Exception {
        Map<KafkaTopicPartition, Long> topicPartitionMap = new HashMap<>();

        AdminClient adminClient = KafkaAdminClient.create(props);
        List<String> allGroups = adminClient.listConsumerGroups()
                .valid()
                .get(10l, TimeUnit.SECONDS)
                .stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toList());

        for (String group : allGroups) {
            ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(group);
            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetAndMetadataMap.entrySet()) {
                TopicPartition topicpartition = entry.getKey(); // 某一個topic的某一個partition
                OffsetAndMetadata offset = entry.getValue(); // offset
                topicPartitionMap.put(new KafkaTopicPartition(topicpartition.topic(), topicpartition.partition()), offset.offset());
            }
        }

        if (adminClient != null) {
            adminClient.close();
        }
        return topicPartitionMap;
    }
}
