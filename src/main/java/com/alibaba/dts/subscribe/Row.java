package com.alibaba.dts.subscribe;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;


@Data
@Accessors(chain = true)
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


    /**
     * 类型
     */
    public enum Type {
        insert, delete, update, replace, ddl, unknown
    }

    @Data
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

    }


}

