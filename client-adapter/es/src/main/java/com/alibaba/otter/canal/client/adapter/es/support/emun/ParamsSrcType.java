package com.alibaba.otter.canal.client.adapter.es.support.emun;

/**
 * @Description 数据类型枚举
 * @Author 黄念
 * @Date 2021/5/11
 * @Version1.0
 */
public enum ParamsSrcType {

    DOC("doc"),
    PARAMS("params");

    public String value;

    ParamsSrcType(String value) {
        this.value = value;
    }

}
