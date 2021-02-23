package org.dxer.flink.canal.client.entity;

import java.io.Serializable;

public class TaskInfo implements Serializable {

    private static final long serialVersionUID = 2611556444074013268L;

    private String srcTable;

    private String srcDatabase;

    private String targetDatabase;

    private String targetTable;

    public String getSrcTable() {
        return srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public String getSrcDatabase() {
        return srcDatabase;
    }

    public void setSrcDatabase(String srcDatabase) {
        this.srcDatabase = srcDatabase;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    @Override
    public String toString() {
        return "TaskInfo{" +
                "srcTable='" + srcTable + '\'' +
                ", srcDatabase='" + srcDatabase + '\'' +
                ", targetDatabase='" + targetDatabase + '\'' +
                ", targetTable='" + targetTable + '\'' +
                '}';
    }
}
