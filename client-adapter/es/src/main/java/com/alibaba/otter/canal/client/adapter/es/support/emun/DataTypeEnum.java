package com.alibaba.otter.canal.client.adapter.es.support.emun;

/**
 * @Description 数据类型枚举
 * @Author 黄念
 * @Date 2021/5/11
 * @Version1.0
 */
public enum DataTypeEnum {
    BYTE("byte"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    CHAR("char"),
    STRING("string"),
    BOOLEAN("boolean"),
    UNDEFINED("undefined");

    public String value;

    DataTypeEnum(String value) {
        this.value = value;
    }

    /*
     * 匹配枚举
     * */
    public static DataTypeEnum match(String str) {
        for (DataTypeEnum typeEnum : DataTypeEnum.values()) {
            if (typeEnum.name().equalsIgnoreCase(str)) {
                return typeEnum;
            }
        }
        return DataTypeEnum.UNDEFINED;
    }
}
