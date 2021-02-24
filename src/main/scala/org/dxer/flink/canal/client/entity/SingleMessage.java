package org.dxer.flink.canal.client.entity;

import org.dxer.flink.canal.client.ConfigConstants;
import org.dxer.flink.canal.client.util.JdbcUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleMessage implements Serializable {

    private static final long serialVersionUID = 2611556444074013268L;

    private String topic;

    private Integer partitionId;

    private Long offset;

    private Map<String, Object> data;

    private Map<String, Object> old;

    private String database;

    private String table;

    private Long es;

    private Long ts;

    private Boolean isDdl;

    private List<String> pkNames;

    private String type;

    private String sql;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public void setOld(Map<String, Object> old) {
        this.old = old;
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

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Boolean getDdl() {
        return isDdl;
    }

    public void setDdl(Boolean ddl) {
        isDdl = ddl;
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

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    /**
     * message 解析，canal 中一条数据可以能包含对多条数据进行操作
     *
     * @param msg
     * @return
     */
    public static List<SingleMessage> message2SingleMessages(Message msg) {
        List<SingleMessage> singleMessages = null;
        if (msg != null) {
            singleMessages = new ArrayList<>();

            if (ConfigConstants.ALTER.equals(msg.getType())) {
                SingleMessage singleMessage = new SingleMessage();

                singleMessage.setDatabase(msg.getDatabase());
                singleMessage.setTable(msg.getTable());
                singleMessage.setSql(msg.getSql());
                singleMessage.setTable(msg.getTable());
                singleMessage.setType(msg.getType());

                if (msg.getSql().contains("DROP COLUMN") || msg.getSql().contains("ADD COLUMN")) {
                    singleMessages.add(singleMessage);
                }
            } else if (ConfigConstants.INSERT.equals(msg.getType()) || ConfigConstants.UPDATE.equals(msg.getType()) || ConfigConstants.DELETE.equals(msg.getType())) {
                List<Map<String, Object>> data = msg.getData();
                // List<Map<String, Object>> old = msg.getOld();
                Map<String, String> mysqlTypes = msg.getMysqlType();
                Map<String, Integer> sqlTypes = msg.getSqlType();
                String table = msg.getTable();

                for (int i = 0; i < data.size(); i++) {
                    SingleMessage singleMessage = new SingleMessage();
                    Map<String, Object> newData = new HashMap<>();

                    for (String columnName : mysqlTypes.keySet()) { // 修改数据类型
                        Integer sqlType = sqlTypes.get(columnName);
                        String mysqlType = mysqlTypes.get(columnName);

                        Object obj = data.get(i).get(columnName);
                        String columnValue = obj != null ? (String) data.get(i).get(columnName) : null;

                        if (mysqlType == null) {
                            newData.put(columnName.toLowerCase(), columnValue); // 没有匹配到对应的 mysqlType, 直接使用原始值
                            continue;
                        }
                        Object finalValue = JdbcUtil.typeConvert(table, columnName, columnValue, sqlType, mysqlType); // 将数据转化为对应的类型
                        newData.put(columnName.toLowerCase(), finalValue); // 统一用小写
                    }

                    singleMessage.setTopic(msg.getTopic());
                    singleMessage.setOffset(msg.getOffset());


                    singleMessage.setData(newData);
                    singleMessage.setDatabase(msg.getDatabase());
                    singleMessage.setTable(msg.getTable());
                    singleMessage.setSql(msg.getSql());
                    singleMessage.setPkNames(msg.getPkNames());
                    singleMessage.setTs(msg.getTs());
                    singleMessage.setEs(msg.getEs());
                    singleMessage.setType(msg.getType());


//                if (old != null) {
//                    singleMessage.setOld(old.get(i));
//                }

                    singleMessages.add(singleMessage);
                }
            }
        }
        return singleMessages;
    }

    @Override
    public String toString() {
        return "SingleMessage{" +
                "topic='" + topic + '\'' +
                ", partitionId=" + partitionId +
                ", offset=" + offset +
                ", data=" + data +
                ", old=" + old +
                ", database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", es=" + es +
                ", ts=" + ts +
                ", isDdl=" + isDdl +
                ", pkNames=" + pkNames +
                ", type='" + type + '\'' +
                ", sql='" + sql + '\'' +
                '}';
    }
}
