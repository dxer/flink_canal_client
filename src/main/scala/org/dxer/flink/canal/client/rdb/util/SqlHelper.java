package org.dxer.flink.canal.client.rdb.util;

import org.dxer.flink.canal.client.rdb.SQLCommand;
import org.dxer.flink.canal.client.entity.SingleMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlHelper {

    public static SQLCommand buildSQL(SingleMessage message, String fullTableName) {
        String type = message.getType();

        if (type.equals("DELETE")) {
            return delete(message, fullTableName);
        } else if (type.equals("INSERT") || type.equals("UPDATE")) {
            return insert(message, fullTableName);
        } else if (type.equals("ALTER")) {
            return alter(message, fullTableName);
        }
        return null;
    }

    public static SQLCommand insert(SingleMessage message, String fullTableName) {
        String table = message.getTable();
        List<String> pkNames = message.getPkNames();
        String type = message.getType();

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ")
                .append(fullTableName)
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
        return new SQLCommand(fullTableName, type, insertSql.toString(), list);
    }

    public static SQLCommand delete(SingleMessage message, String fullTableName) {
        StringBuilder deleteSql = new StringBuilder();
        String table = message.getTable();
        List<String> pkNames = message.getPkNames();
        String type = message.getType();


        Map<String, Object> data = message.getData();

        List<Object> values = new ArrayList<>();

        deleteSql.append("DELETE FROM ").append(fullTableName).append(" WHERE ");
        for (String key : pkNames) {
            deleteSql.append(key).append("=? AND ");
            values.add(data.get(key));
        }
        int len = deleteSql.length();
        deleteSql.delete(len - 4, len);

        return new SQLCommand(fullTableName, type, deleteSql.toString(), values);
    }

    private static SQLCommand alter(SingleMessage message, String fullTableName) {
        return new SQLCommand(fullTableName, message.getType(), message.getSql(), null);
    }

}
