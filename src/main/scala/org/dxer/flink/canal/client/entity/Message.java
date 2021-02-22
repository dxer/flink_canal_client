package org.dxer.flink.canal.client.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Message implements Serializable {

    private static final long serialVersionUID = 2611556444074013268L;

    private List<Map<String, Object>> data;

    private List<Map<String, Object>> old;

    private String database;

    private String table;

    private Long es;

    private Long ts;

    private String id;

    private Boolean isDdl;

    private Map<String, String> mysqlType;

    private Map<String,Integer> sqlType;

    private List<String> pkNames;

    private String type;

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public List<Map<String, Object>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, Object>> old) {
        this.old = old;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Boolean getDdl() {
        return isDdl;
    }

    public void setDdl(Boolean ddl) {
        isDdl = ddl;
    }

    public Map<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public Map<String, Integer> getSqlType() {
        return sqlType;
    }

    public void setSqlType(Map<String, Integer> sqlType) {
        this.sqlType = sqlType;
    }


    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


}
