package com.alibaba.dts.subscribe;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;


@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonFormat(shape = JsonFormat.Shape.NUMBER)
public class Row implements Serializable {

    private final static long serialVersionUID = 262362438424988816L;


    /**
     * 数据库名称
     */
    @JsonProperty("database")
    private String database;

    /**
     * 表名称
     */
    @JsonProperty("table")
    private String table;

    /**
     * 类型 insert,delete,update,ddl
     */
    @JsonProperty(value = "type")
    private Type type;

    /**
     * 数据
     */
    @JsonProperty("data")
    private List<Column> data;

    /**
     * 更改前的数据,仅对delete,update有效
     */
    @JsonProperty("old")
    private List<Column> old;

    /**
     * 主键
     */
    private List<Column> primaryKeys;

    /**
     * DDL语句
     */
    @JsonProperty("sql")
    private String sql;


    public String getDatabase() {
        return database;
    }

    public Row setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getTable() {
        return table;
    }

    public Row setTable(String table) {
        this.table = table;
        return this;
    }

    public Type getType() {
        return type;
    }

    public Row setType(Type type) {
        this.type = type;
        return this;
    }

    public List<Column> getData() {
        return data;
    }

    public Row setData(List<Column> data) {
        this.data = data;
        return this;
    }

    public List<Column> getOld() {
        return old;
    }

    public void setOld(List<Column> old) {
        this.old = old;
    }

    public List<Column> getPrimaryKeys() {
        return primaryKeys;
    }

    public Row setPrimaryKeys(List<Column> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public String getSql() {
        return sql;
    }

    public Row setSql(String sql) {
        this.sql = sql;
        return this;
    }

    /**
     * 类型
     */
    public enum Type {
        insert, delete, update, replace, ddl, unknown
    }

    public static class Column {

        /**
         * 字段名称
         */
        private String name;

        /**
         * 值
         */
        private Object value;

        /**
         * 类型 例 text,varchar,int
         */
        private String type;

        /**
         * 是否主键
         */
        private boolean primaryKey;

        public String getName() {
            return name;
        }

        public Column setName(String name) {
            this.name = name;
            return this;
        }

        public Object getValue() {
            return value;
        }

        public Column setValue(Object value) {
            this.value = value;
            return this;
        }

        public String getType() {
            return type;
        }

        public Column setType(String type) {
            this.type = type;
            return this;
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public Column setPrimaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }
    }


}

