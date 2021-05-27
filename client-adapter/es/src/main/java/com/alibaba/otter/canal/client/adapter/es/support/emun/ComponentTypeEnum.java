package com.alibaba.otter.canal.client.adapter.es.support.emun;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/22
 * @Version1.0
 */
public enum ComponentTypeEnum {
    ES("es"),
    MYSQL("mysql"),
    UNDEFINED("undefined");

    public String value;

    ComponentTypeEnum(String value) {
        this.value = value;
    }

    /*
     * 匹配枚举
     * */
    public static ComponentTypeEnum match(String str) {
        for (ComponentTypeEnum typeEnum : ComponentTypeEnum.values()) {
            if (typeEnum.name().equalsIgnoreCase(str)) {
                return typeEnum;
            }
        }
        return ComponentTypeEnum.UNDEFINED;
    }
}
