package com.alibaba.otter.canal.client.adapter.es.support.processor.post;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OperationEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description 后置处理
 * @Author 黄念
 * @Date 2021/5/25
 * @Version1.0
 */
public abstract class Postprocessor {
    Logger logger = LoggerFactory.getLogger(Postprocessor.class);
    JdbcTemplate jdbcTemplate = new JdbcTemplate();

    private static Map<String, Postprocessor> postprocessorMap = new ConcurrentHashMap<>();

    public static void init() throws Exception {
        for (Class<?> cls : ESSyncUtil.getClasses(Postprocessor.class.getPackage().getName())) {
            Service annotation = cls.getAnnotation(Service.class);
            if (annotation != null) {
                Constructor<?> constructor = cls.getConstructor();
                Postprocessor postprocessor = (Postprocessor) constructor.newInstance();
                postprocessorMap.put(cls.getSimpleName().toLowerCase(), postprocessor);
            }
        }
        //System.out.println(postprocessorMap);
    }

    public static Postprocessor getInstance(String name) {
        return postprocessorMap.get(name);
    }

    public abstract void dispose(ESSyncConfig esSyncConfig, Map<String, Object> sourceData, Map<String, Object> esFieldData, OperationEnum operationEnum);

}
