package org.dxer.flink.canal.client;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.dxer.flink.canal.client.entity.KafkaAndPartitionInfo;
import org.dxer.flink.canal.client.entity.TaskInfo;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.*;

public class AppConfig implements Serializable {

    private static final long serialVersionUID = 2611556444074013268L;

    private String file = null;
    private Map<String, Object> map = new HashMap<>();

    private Map<String, String> dbMapping = new HashMap<>();

    private List<TaskInfo> taskInfos = new ArrayList<>();

    public AppConfig(String file) {
        this.file = file;
    }

    public void init() throws Exception {
        Yaml yaml = new Yaml();
        map = yaml.load(new FileInputStream(file));
        initTasks();
    }


    public Map<String, Object> getAll() {
        return map;
    }

    public Object get(String key) {
        return map.get(key);
    }

    public String getString(String key) {
        Object obj = get(key);
        return obj != null ? (String) obj : null;
    }

    public Integer getInt(String key) {
        Object obj = get(key);
        return obj != null ? (Integer) obj : null;
    }

    public Long getLong(String key) {
        Object obj = get(key);
        return obj != null ? (Long) obj : null;
    }

    public Map getMap(String key) {
        Object obj = get(key);
        return obj != null ? (Map) obj : null;
    }

    public List getList(String key) {
        Object obj = get(key);
        return obj != null ? (ArrayList) obj : null;
    }

    public String getAppName() {
        return getString(ConfigConstants.APP_NAME);
    }

    public int getThreadNum() {
        return getInt(ConfigConstants.THREAD_NUM);
    }

    public Properties getKafkaSourceProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", getString(ConfigConstants.SOURCE_KAFKA_BOOTSTRAP_SERVERS)); // bootstrap server
        props.setProperty("group.id", getString(ConfigConstants.SOURCE_KAFKA_GROUP_ID)); // group id
        props.setProperty("auto.offset.reset", "earliest"); // 当分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费

        // TODO kerberos

        return props;
    }

    public KafkaAndPartitionInfo getKafkaAndPartitionInfo() {
        KafkaAndPartitionInfo info = null;
        List<Map<String, String>> list = getList(ConfigConstants.SOURCE_KAFKA_TOPICS);

        if (list == null) return null;

        info = new KafkaAndPartitionInfo();
        List<String> topics = new ArrayList<>();
        Map<String, String> startupModes = new HashMap<>();
        Map<String, String> specificOffsets = new HashMap<>();
        Map<String, Long> timestampOffsets = new HashMap<>();

        for (int i = 0; i < list.size(); i++) {
            Map<String, String> map = list.get(i);

            String topic = map.get(ConfigConstants.SOURCE_KAFKA_TOPIC);
            topics.add(topic);

            String startupMode = map.get(ConfigConstants.STARTUP_MODE);
            if (!Strings.isNullOrEmpty(startupMode)) {
                switch (startupMode) {
                    case ConfigConstants.STARTUP_MODE_EARLIEST:
                    case ConfigConstants.STARTUP_MODE_LATEST:
                        startupModes.put(topic, startupMode);
                        break;
                    case ConfigConstants.STARTUP_MODE_TIMESTAMP:
                        String timestamp = map.get(ConfigConstants.STARTUP_TIMESTAMP);
                        if (!Strings.isNullOrEmpty(timestamp)) {
                            timestampOffsets.put(topic, Long.parseLong(map.get(ConfigConstants.STARTUP_TIMESTAMP)));
                        }
                        break;
                    case ConfigConstants.STARTUP_MODE_SPECIFIC:
                        String offsets = map.get(ConfigConstants.STARTUP_SPECIFIC_OFFSETS);
                        if (!Strings.isNullOrEmpty(offsets)) {
                            specificOffsets.put(topic, offsets);
                        }
                        break;
                    default:
                }
            }
        }

        info.setTopics(topics);
        info.setSpecificOffsets(specificOffsets);
        info.setStartupModes(startupModes);
        info.setTimestampOffsets(timestampOffsets);

        return info;
    }


    private void initTasks() {
        List<Map<String, LinkedHashMap>> list = getList(ConfigConstants.SYNC_TASKS);
        if (list == null) return;

        for (int i = 0; i < list.size(); i++) {
            Map<String, LinkedHashMap> map = list.get(i);
            LinkedHashMap<String, String> source = map.get(ConfigConstants.TASK_SOURCE);
            LinkedHashMap<String, String> target = map.get(ConfigConstants.TASK_TARGET);

            System.out.println(source);
            System.out.println(target);

            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setSrcDatabase(source.get(ConfigConstants.TASK_DATABASE));
            taskInfo.setSrcTable(source.get(ConfigConstants.TASK_TABLE));
            taskInfo.setTargetDatabase(target.get(ConfigConstants.TASK_DATABASE));
            taskInfo.setTargetTable(target.get(ConfigConstants.TASK_TABLE));

            dbMapping.put(taskInfo.getSrcDatabase() + "." + taskInfo.getSrcTable(), taskInfo.getTargetDatabase() + "." + taskInfo.getTargetTable());

            taskInfos.add(taskInfo);
        }
    }

    public List<TaskInfo> getTaskInfos() {
        return this.taskInfos;
    }

    public Map<String, String> getDBMappings() {
        return this.dbMapping;
    }
}
