package com.alibaba.otter.canal.client.adapter.es.support;

import lombok.SneakyThrows;

import java.lang.reflect.Method;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/11/3
 * @Version1.0
 */
public class EnumUtils {
    /**
     * 通过枚举的value获取对应的name
     *
     * @param cls
     * @param value
     * @param <T>
     * @param <U>
     * @return
     */
    public static <T extends Enum<T>, U extends Comparable<U>> String getName(Class<T> cls, U value) throws Exception {
        T instance = getInstance(cls, value);
        return instance.name();
    }

    /**
     * 通过枚举的value获取对应的实例
     * 需要存在getValue方法.
     *
     * @param cls   枚举类
     * @param value 枚举value字段值
     * @param <T>
     * @param <U>
     * @return
     */
    @SneakyThrows
    public static <T extends Enum<T>, U extends Comparable<U>> T getInstance(Class<T> cls, U value){
       return getInstance(cls,value,"getValue");
    }

    /**
     * 通过枚举的value获取对应的实例
     * 需要存在getValue方法.
     * @param cls   枚举类
     * @param value 枚举字段值
     * @param <T>
     * @param <U>
     * @return
     */
    public static <T extends Enum<T>, U extends Comparable<U>> T getInstance(Class<T> cls, U value, String methodName) throws Exception {
        Method getValue = cls.getMethod(methodName);
        for (T constant : cls.getEnumConstants()) {
            U val = (U) getValue.invoke(constant);
            if (val.compareTo(value) == 0) {
                return constant;
            }
        }
        System.err.println(String.format("无法找到对应的枚举实例,  类名: %s, value: %s, 方法名: %s", cls.getName(), value, methodName));
        throw new RuntimeException("无法找到对应的枚举实例");
    }
}
