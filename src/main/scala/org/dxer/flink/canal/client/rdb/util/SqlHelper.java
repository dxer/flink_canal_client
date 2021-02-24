package org.dxer.flink.canal.client.rdb.util;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.dxer.flink.canal.client.entity.SingleMessage;
import org.dxer.flink.canal.client.rdb.SQLRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlHelper {

    public static SQLRequest buildSQL(SingleMessage message, String fullTableName) {
        SQLRequest req = null;
        String type = message.getType();
        if (type.equals("DELETE")) {
            req = delete(message, fullTableName);
        } else if (type.equals("INSERT") || type.equals("UPDATE")) {
            req = insert(message, fullTableName);
        } else if (type.equals("ALTER") && !Strings.isNullOrEmpty(message.getSql())) {
            req = alter(message, fullTableName);
        }
        return req;
    }

    public static SQLRequest insert(SingleMessage message, String fullTableName) {
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
        return new SQLRequest(fullTableName, type, insertSql.toString(), list);
    }

    public static SQLRequest delete(SingleMessage message, String fullTableName) {
        StringBuilder deleteSql = new StringBuilder();
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

        return new SQLRequest(fullTableName, type, deleteSql.toString(), values);
    }

    private static SQLRequest alter(SingleMessage message, String fullTableName) {
        String database = message.getDatabase();
        String table = message.getTable();
        String replaceBefore = String.format("`%s`.`%s`", database, table);
        String sql = message.getSql().replace(replaceBefore, fullTableName);
        return new SQLRequest(fullTableName, message.getType(), sql, null);
    }

}
