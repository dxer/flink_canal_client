package org.dxer.flink.canal.client.entity;

import java.util.List;
import java.util.Map;

public class KafkaAndPartitionInfo {

    private List<String> topics;

    private Map<String, String> specificOffsets;

    private Map<String, String> startupModes;

    private Map<String, Long> timestampOffsets;

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public Map<String, String> getSpecificOffsets() {
        return specificOffsets;
    }

    public void setSpecificOffsets(Map<String, String> specificOffsets) {
        this.specificOffsets = specificOffsets;
    }

    public Map<String, String> getStartupModes() {
        return startupModes;
    }

    public void setStartupModes(Map<String, String> startupModes) {
        this.startupModes = startupModes;
    }

    public Map<String, Long> getTimestampOffsets() {
        return timestampOffsets;
    }

    public void setTimestampOffsets(Map<String, Long> timestampOffsets) {
        this.timestampOffsets = timestampOffsets;
    }
}
