package com.alibaba.otter.canal.client.adapter.es.support.processor.pre;

import com.alibaba.otter.canal.client.adapter.es.support.processor.pre.impl.BusinessTimeProcessor;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public class PreprocessorFactory {
    private static Map<String, Preprocessor> map = new HashMap<>();

    static {
        map.put("businessTimeProcessor", new BusinessTimeProcessor());
    }

    public static Preprocessor getInstance(String name) {
        return map.get(name);
    }
}
