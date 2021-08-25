package com.alibaba.otter.canal.client.adapter.es.support.mapper;

import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.model.MapperContext;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/15
 * @Version1.0
 */
public class MapperFactory implements Mapper {
    private static MapperFactory mapperFactory = new MapperFactory();

    private static Map<String, AbstractMapper> mapperMap = new HashMap<>();

    private static DefaultMapper defaultMapper = new DefaultMapper();

    public static void init() throws Exception {
        for (Class<?> cls : ESSyncUtil.getClasses(Mapper.class.getPackage().getName())) {
            Service annotation = cls.getAnnotation(Service.class);
            if (annotation != null) {
                Constructor<?> constructor = cls.getConstructor();
                AbstractMapper mapper = (AbstractMapper) constructor.newInstance();
                mapperMap.put(cls.getSimpleName().toLowerCase(), mapper);
            }
        }
    }

    @Override
    public void mapping(ESData esData, MapperContext context) {
        Mapper mapper = mapperMap.get(context.getEsMapping().getConfigFileName());
        if (mapper != null) mapper.mapping(esData, context);
        else defaultMapper.mapping(esData, context);
    }

    public static MapperFactory getInstance() {
        return mapperFactory;
    }

}
