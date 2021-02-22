package org.dxer.flink.canal.client.util;

import org.dxer.flink.canal.client.entity.RowData;
import org.dxer.flink.canal.client.entity.SingleMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlHelper {

    public static RowData buildSQL(SingleMessage message) {
        String type = message.getType();

        if (type.equals("DELETE")) {
            return null;
        } else if (type.equals("INSERT") || type.equals("UPDATE")) {
            return insert(message);
        }
        return null;
    }

    public static RowData insert(SingleMessage message) {
        String table = message.getTable();
        List<String> pkNames = message.getPkNames();
        String type = message.getType();

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ")
                .append(message.getTable())
                .append(" (");

        StringBuilder values = new StringBuilder();
        StringBuilder update = new StringBuilder();

        List<Object> list = new ArrayList<>();
        List<Object> list1 = new ArrayList<>();

        for (String key : message.getData().keySet()) {
            if (values.toString().length() > 0) {
                insertSql.append(", ");
                values.append(", ");
            }
            insertSql.append(key);
            values.append("?");
            list.add(message.getData().get(key));

            if (!pkNames.contains(key)) { // 非主键
                if (update.toString().length() > 0) {
                    update.append(", ");
                }
                update.append(key).append("=?");
                list1.add(message.getData().get(key));
            }
        }
        insertSql.append(") VALUES (").append(values.toString()).append(")");

        insertSql.append(" ON DUPLICATE KEY UPDATE ")
                .append(update.toString());
        list.addAll(list1);
        return new RowData(table, type, insertSql.toString(), list);
    }

    public static RowData deleteSQLPstmtMDBuilder(SingleMessage message) {
        StringBuilder deleteSql = new StringBuilder();
        String table = message.getTable();
        List<String> pkNames = message.getPkNames();
        String type = message.getType();


        Map<String, Object> data = message.getData();

        List<Object> values = new ArrayList<>();

        deleteSql.append("DELETE FROM ").append(table).append(" WHERE ");
        for (String key : pkNames) {
            deleteSql.append(key).append("=? AND ");
            values.add(data.get(key));
        }
        int len = deleteSql.length();
        deleteSql.delete(len - 4, len);

        return new RowData(table, type, deleteSql.toString(), values);
    }

}
