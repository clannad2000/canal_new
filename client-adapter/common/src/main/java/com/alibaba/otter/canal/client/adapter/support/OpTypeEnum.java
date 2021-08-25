package com.alibaba.otter.canal.client.adapter.support;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/2/1
 * @Version1.0
 */
public enum OpTypeEnum {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete"),
    SCRIPTED_UPDATE("scriptedUpdate"),
    UPDATE_BY_QUERY("updateByUpdate"),
    NONE("none");
    public String value;

    OpTypeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
