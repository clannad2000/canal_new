package com.alibaba.otter.canal.client.adapter.es.support.processor.updateByQuery;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/25
 * @Version1.0
 */
public abstract class UpdateByQueryBuilder {
    protected Logger logger = LoggerFactory.getLogger(UpdateByQueryBuilder.class);
    protected JdbcTemplate jdbcTemplate = new JdbcTemplate();

    private static Map<String, UpdateByQueryBuilder> updateByQueryBuilderMap = new ConcurrentHashMap<>();

    public static UpdateByQueryBuilder getInstance(String name) {
        return updateByQueryBuilderMap.get(name);
    }

    public static void init() throws Exception {
        for (Class<?> cls : ESSyncUtil.getClasses(UpdateByQueryBuilder.class.getPackage().getName())) {
            Service annotation = cls.getAnnotation(Service.class);
            if (annotation != null) {
                Constructor<?> constructor = cls.getConstructor();
                UpdateByQueryBuilder postprocessor = (UpdateByQueryBuilder) constructor.newInstance();
                updateByQueryBuilderMap.put(cls.getSimpleName().toLowerCase(), postprocessor);
            }
        }
        //System.out.println(updateByQueryBuilderMap);
    }

    public abstract UpdateByQueryInfo build(Map<String, Object> esFieldData, ESSyncConfig.ESMapping mapping);
}
