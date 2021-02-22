package org.dxer.flink.canal.client;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.util.*;

public class AppConfig {

    private String file = null;
    private Map<String, Object> map = new HashMap<>();

    public AppConfig(String file) {
        this.file = file;
    }

    public void init() throws Exception {
        Yaml yaml = new Yaml();
        map = yaml.load(new FileInputStream(file));
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

    public List getList(String key) {
        Object obj = get(key);
        return obj != null ? (ArrayList) obj : null;
    }

    public String getAppName() {
        return getString(ConfigConstants.APP_NAME);
    }

    public Properties getKafkaSourceProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", getString(ConfigConstants.SOURCE_KAFKA_BOOTSTRAP_SERVERS)); // bootstrap server
        props.setProperty("group.id", getString(ConfigConstants.SOURCE_KAFKA_GROUP_ID)); // group id
        props.setProperty("auto.offset.reset", "earliest"); // 当分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费

        // TODO kerberos

        return props;
    }


    public List<String> getKafkaTopics() {
        Object o = get(ConfigConstants.SOURCE_KAFKA_TOPICS);
        if (o != null) {
            if (o instanceof String) {
                List<String> list = new ArrayList<>();
                String[] strings = o.toString().split(",");
                for (int i = 0; i < strings.length; i++) {
                    list.add(strings[i].trim());
                }
            } else {
                return (ArrayList) o;
            }
        }
        return null;
    }

    public List<String> getSyncTables() {
        return null;
    }
}
