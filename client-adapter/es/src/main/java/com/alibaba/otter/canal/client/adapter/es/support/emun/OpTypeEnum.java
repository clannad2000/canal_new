package com.alibaba.otter.canal.client.adapter.es.support.emun;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/2/1
 * @Version1.0
 */
public enum OpTypeEnum {
    INSERT("insert"), UPDATE("update"), DELETE("delete");

    public String value;

    OpTypeEnum(String value) {
        this.value = value;
    }
}
