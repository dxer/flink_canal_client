package org.dxer.flink.canal.client.entity;

import java.io.Serializable;
import java.util.List;

public class RowData implements Serializable {

    private static final long serialVersionUID = 2611556444074013268L;

    private String table;
    private String sql;
    private List<Object> values;

    public RowData(String table, String sql, List<Object> values) {
        this.table = table;
        this.sql = sql;
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
}
