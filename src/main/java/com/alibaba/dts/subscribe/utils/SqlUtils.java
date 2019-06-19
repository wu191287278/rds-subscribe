package com.alibaba.dts.subscribe.utils;

import com.alibaba.dts.subscribe.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class SqlUtils {

    private static final Map<Integer, Class<?>> SQL_TYPE_TO_JAVA_TYPE_MAP = new HashMap<>();

    private static final Set<String> DATE_TYPES = new HashSet<>(Arrays.asList(
            "DATETIME", "TIMESTAMP", "DATE", "YEAR", "TIME", "DATE-TIME",
            "datetime", "timestamp", "date", "year", "time", "date-time"
    ));

    private static final Set<String> BLOB_TYPES = new HashSet<>(Arrays.asList(
            "TINYBLOB", "tinyblob", "BLOB", "blob", "MEDIUMBLOB", "mediumblob", "LONGBLOB", "longblob"
    ));

    static {
        // bool
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.BOOLEAN, Boolean.class);

        // int
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.TINYINT, Integer.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.SMALLINT, Integer.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.INTEGER, Integer.class);

        // long
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.BIGINT, Long.class);
        // mysql bit最多64位，无符号
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.BIT, BigInteger.class);

        // decimal
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.REAL, Float.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.FLOAT, Float.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.DOUBLE, Double.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.NUMERIC, BigDecimal.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.DECIMAL, BigDecimal.class);

        // date
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.DATE, java.sql.Date.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.TIME, Time.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.TIMESTAMP, Timestamp.class);

        // blob
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.BLOB, byte[].class);

        // byte[]
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.REF, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.OTHER, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.ARRAY, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.STRUCT, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.SQLXML, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.BINARY, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.DATALINK, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.DISTINCT, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.VARBINARY, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.JAVA_OBJECT, byte[].class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.LONGVARBINARY, byte[].class);

        // String
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.CHAR, String.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.VARCHAR, String.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.LONGVARCHAR, String.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.LONGNVARCHAR, String.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.NCHAR, String.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.NVARCHAR, String.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.NCLOB, String.class);
        SQL_TYPE_TO_JAVA_TYPE_MAP.put(Types.CLOB, String.class);
    }


    public static String rollbackSql(Row row) {
        List<Row.Column> old = row.getOld();
        List<Row.Column> data = row.getData();
        row.setData(old)
                .setOld(data);
        if (row.getType() == Row.Type.delete) {
            row.setType(Row.Type.insert);
        }

        if (row.getType() == Row.Type.insert) {
            row.setType(Row.Type.delete);
        }

        return toSql(row, true);
    }

    public static String toSql(Row row) {
        return toSql(row, true);
    }

    public static String toSql(Row row, boolean containsDatasourceName) {

        if (row.getType() == Row.Type.ddl) {
            return row.getSql();
        }

        switch (row.getType()) {
            case insert:
                return insert(row, containsDatasourceName);
            case delete:
                return delete(row, containsDatasourceName);
            case update:
                return update(row, containsDatasourceName);
        }
        return null;
    }


    public static String toDeleteSql(Row row) {

        List<Row.Column> data = null;
        if (row.getData() != null) {
            data = row.getData();
        }

        if (row.getOld() != null) {
            data = row.getOld();
        }

        if (data == null) {
            return null;
        }

        String table = row.getDatabase() + "." + row.getTable();
        StringBuilder sb = new StringBuilder("delete from ")
                .append(table)
                .append(" where ");
        List<Row.Column> primaryKeys = row.getPrimaryKeys();
        if (primaryKeys.size() == 1) {
            Row.Column column = primaryKeys.get(0);
            sb.append(column.getName())
                    .append("=")
                    .append(valueToStr(column.getValue(), column.getType()));
        } else if (primaryKeys.size() > 1) {
            Row.Column column = primaryKeys.get(0);
            String value = valueToStr(column.getValue(), column.getType());
            sb.append(column.getName())
                    .append("=")
                    .append(value)
                    .append(" ");
            for (int i = 1; i < primaryKeys.size(); i++) {
                column = primaryKeys.get(0);
                value = valueToStr(column.getValue(), column.getType());
                sb.append(" and ")
                        .append(column.getName())
                        .append("=")
                        .append(value)
                        .append(" ");
            }
        } else {
            Row.Column column = data.get(0);
            String value = valueToStr(column.getValue(), column.getType());
            sb.append(column.getName())
                    .append("=")
                    .append(value)
                    .append(" ");
            for (int i = 1; i < data.size(); i++) {
                column = data.get(0);
                value = valueToStr(column.getValue(), column.getType());
                sb.append(" and ")
                        .append(column.getName())
                        .append("=")
                        .append(value)
                        .append(" ");
            }
        }


        return sb.toString();
    }


    private static String update(Row row, boolean containsDatasourceName) {
        List<Row.Column> data = row.getData();
        List<Row.Column> old = row.getOld();
        String table = containsDatasourceName ? (row.getDatabase() + "." + row.getTable()) : row.getTable();

        Set<String> primaryKeys = row.getPrimaryKeys()
                .stream()
                .map(Row.Column::getName)
                .collect(Collectors.toSet());

        StringBuilder sb = new StringBuilder("update ")
                .append(table)
                .append(" set ");
        List<String> setValues = new ArrayList<>();
        for (Row.Column column : data) {
            String value = valueToStr(column.getValue(), column.getType());
            setValues.add(column.getName() + "=" + value);
        }

        sb.append(String.join(", ", setValues));

        List<Row.Column> conditions = old.stream()
                .filter(c -> primaryKeys.contains(c.getName()))
                .collect(Collectors.toList());
        sb.append(" where ");
        if (conditions.size() > 0) {
            Row.Column column = conditions.get(0);
            sb.append(column.getName())
                    .append("=")
                    .append(valueToStr(column.getValue(), column.getType()));
        }

        if (conditions.size() > 1) {
            for (int i = 1; i < conditions.size(); i++) {
                Row.Column column = conditions.get(i);
                sb.append(" and ")
                        .append(column.getName())
                        .append("=")
                        .append(valueToStr(column.getValue(), column.getType()));
            }
        }

        return sb.toString();


    }


    private static String delete(Row row, boolean containsDatasourceName) {
        if (row.getOld() == null) {
            return null;
        }

        String table = containsDatasourceName ? (row.getDatabase() + "." + row.getTable()) : row.getTable();
        StringBuilder sb = new StringBuilder("delete from ")
                .append(table)
                .append(" where ");
        List<Row.Column> primaryKeys = row.getPrimaryKeys();
        if (primaryKeys.size() == 1) {
            Row.Column column = primaryKeys.get(0);
            sb.append(column.getName())
                    .append("=")
                    .append(valueToStr(column.getValue(), column.getType()));
        } else if (primaryKeys.size() > 1) {
            Row.Column column = primaryKeys.get(0);
            String value = valueToStr(column.getValue(), column.getType());
            sb.append(column.getName())
                    .append("=")
                    .append(value)
                    .append(" ");
            for (int i = 1; i < primaryKeys.size(); i++) {
                column = primaryKeys.get(i);
                value = valueToStr(column.getValue(), column.getType());
                sb.append(" and ")
                        .append(column.getName())
                        .append("=")
                        .append(value)
                        .append(" ");
            }
        } else {
            List<Row.Column> old = row.getOld();
            Row.Column column = old.get(0);
            String value = valueToStr(column.getValue(), column.getType());
            sb.append(column.getName())
                    .append("=")
                    .append(value)
                    .append(" ");
            for (int i = 1; i < old.size(); i++) {
                column = old.get(i);
                value = valueToStr(column.getValue(), column.getType());
                sb.append(" and ")
                        .append(column.getName())
                        .append("=")
                        .append(value)
                        .append(" ");
            }
        }


        return sb.toString();
    }

    private static String insert(Row row, boolean containsDatasourceName) {
        StringBuilder sb = new StringBuilder();

        String table = containsDatasourceName ? (row.getDatabase() + "." + row.getTable()) : row.getTable();

        List<Row.Column> data = row.getData();
        if (data == null) return null;
        String columnNames = row.getData()
                .stream()
                .map(Row.Column::getName)
                .collect(Collectors.joining(", "));
        String value = data.stream()
                .map(c -> valueToStr(c.getValue(), c.getType()))
                .collect(Collectors.joining(", "));
        return sb.append("insert into ")
                .append(table)
                .append("(")
                .append(columnNames)
                .append(")")
                .append(" values ")
                .append("(")
                .append(value)
                .append(")")
                .toString();
    }


    public static String toReplaceSql(Row row) {
        return toReplaceSql(row, false);
    }

    public static String toReplaceSql(Row row, boolean containsDatasourceName) {
        StringBuilder sb = new StringBuilder();

        String table = containsDatasourceName ? (row.getDatabase() + "." + row.getTable()) : row.getTable();

        List<Row.Column> data = row.getData();
        if (data == null) return null;
        String columnNames = row.getData()
                .stream()
                .map(Row.Column::getName)
                .collect(Collectors.joining(", "));
        String value = data.stream()
                .map(c -> valueToStr(c.getValue(), c.getType()))
                .collect(Collectors.joining(", "));
        return sb.append("replace into ")
                .append(table)
                .append("(")
                .append(columnNames)
                .append(")")
                .append(" values ")
                .append("(")
                .append(value)
                .append(")")
                .toString();
    }


    private static String valueToStr(Object value, String type) {
        if (value == null) {
            return null;
        }

        if (value instanceof byte[]) {
            String hex = HexUtils.toHexString((byte[]) value);
            return "0x" + hex;
        }

        if (isBinaryType(type)) {
            return String.valueOf(value);
        }

        if (value instanceof String) {
            return valueEscape((String) value);
        }

        if (value instanceof Date) {
            value = ((Date) value).getTime();
        }


        if (isDate(type) && value instanceof Long) {
            LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli((long) value), ZoneId.systemDefault());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            return "'" + formatter.format(time) + "'";
        }

        return String.valueOf(value);
    }

    public static boolean isDate(String type) {
        return type != null && DATE_TYPES.contains(type);
    }

    public static boolean isDate(int sqlType) {
        return Types.DATE == sqlType || Types.TIME == sqlType || Types.TIMESTAMP == sqlType;
    }


    public static boolean isNumeric(int sqlType) {
        return (Types.BIT == sqlType) || (Types.BIGINT == sqlType) || (Types.DECIMAL == sqlType)
                || (Types.DOUBLE == sqlType) || (Types.FLOAT == sqlType) || (Types.INTEGER == sqlType)
                || (Types.NUMERIC == sqlType) || (Types.REAL == sqlType) || (Types.SMALLINT == sqlType)
                || (Types.TINYINT == sqlType);
    }

    public static boolean isTextType(int sqlType) {
        return sqlType == Types.CHAR || sqlType == Types.VARCHAR || sqlType == Types.CLOB || sqlType == Types.LONGVARCHAR
                || sqlType == Types.NCHAR || sqlType == Types.NVARCHAR || sqlType == Types.NCLOB
                || sqlType == Types.LONGNVARCHAR;
    }

    public static boolean isBinaryType(String type) {
        return BLOB_TYPES.contains(type);
    }

    public static Class<?> getJavaType(int jdbcType) {
        return SQL_TYPE_TO_JAVA_TYPE_MAP.get(jdbcType);
    }

    public static String valueEscape(String x) {
        StringBuilder buf = new StringBuilder((int) (x.length() * 1.1));
        buf.append('\'');
        for (int i = 0; i < x.length(); ++i) {
            char c = x.charAt(i);

            switch (c) {
                case 0: /* Must be escaped for 'mysql' */
                    buf.append('\\');
                    buf.append('0');

                    break;

                case '\n': /* Must be escaped for logs */
                    buf.append('\\');
                    buf.append('n');

                    break;

                case '\r':
                    buf.append('\\');
                    buf.append('r');

                    break;

                case '\\':
                    buf.append('\\');
                    buf.append('\\');

                    break;

                case '\'':
                    buf.append('\\');
                    buf.append('\'');

                    break;

                case '"': /* Better safe than sorry */
                    buf.append('\\');
                    buf.append('"');

                    break;

                case '\032': /* This gives problems on Win32 */
                    buf.append('\\');
                    buf.append('Z');

                    break;

                case '\u00a5':
                case '\u20a9':
                default:
                    buf.append(c);
            }
        }

        buf.append('\'');
        return buf.toString();
    }


}
