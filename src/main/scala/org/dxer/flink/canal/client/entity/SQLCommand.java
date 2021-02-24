package org.dxer.flink.canal.client.entity;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.dxer.flink.canal.client.ConfigConstants;

import java.io.Serializable;
import java.util.List;


public class SQLCommand implements Serializable {

    private static final long serialVersionUID = 2611556444074013268L;

    private String table;
    private String sql;
    private String type;
    private List<Object> values;

    public SQLCommand(String table, String type, String sql, List<Object> values) {
        this.table = table;
        this.sql = sql;
        this.type = type;
        this.values = values;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Boolean isValid() {
        return ConfigConstants.allowTypes.contains(this.table) &&
                !Strings.isNullOrEmpty(sql) &&
                !Strings.isNullOrEmpty(table);
    }

    @Override
    public String toString() {
        return "SQLCommand{" +
                "table='" + table + '\'' +
                ", sql='" + sql + '\'' +
                ", type='" + type + '\'' +
                ", values=" + values +
                '}';
    }
}
