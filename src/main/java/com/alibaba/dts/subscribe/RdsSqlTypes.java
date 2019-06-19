package com.alibaba.dts.subscribe;

import java.util.HashMap;
import java.util.Map;

public class RdsSqlTypes {

    private static final Map<Integer, String> TYPES = new HashMap<>();

    static {
        TYPES.put(0, "DECIMAL");
        TYPES.put(246, "DECIMAL");
        TYPES.put(1, "INT");
        TYPES.put(2, "INT");
        TYPES.put(3, "INT");
        TYPES.put(8, "INT");
        TYPES.put(9, "INT");
        TYPES.put(4, "FLOAT");
        TYPES.put(5, "DOUBLE");
        TYPES.put(6, "NULL");
        TYPES.put(7, "TIMESTAMP");
        TYPES.put(17, "TIMESTAMP");
        TYPES.put(18, "DATETIME");
        TYPES.put(12, "DATETIME");
        TYPES.put(10, "DATE");
        TYPES.put(14, "DATE");
        TYPES.put(11, "TIME");
        TYPES.put(19, "TIME");
        TYPES.put(13, "YEAR");
        TYPES.put(15, "VARCHAR");
        TYPES.put(253, "VARCHAR");
        TYPES.put(254, "VARCHAR");
        TYPES.put(16, "BIT");
        TYPES.put(245, "JSON");
        TYPES.put(247, "ENUM");
        TYPES.put(248, "SET");
        TYPES.put(249, "TINY_BLOB");
        TYPES.put(250, "MEDIUM_BLOB");
        TYPES.put(251, "LONG_BLOB");
        TYPES.put(252, "BLOB");
        TYPES.put(255, "GEOMETRY");
    }

    public static String getType(int type) {
        return TYPES.get(type);
    }

}
