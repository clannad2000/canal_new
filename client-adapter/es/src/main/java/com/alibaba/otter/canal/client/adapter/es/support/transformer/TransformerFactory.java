package com.alibaba.otter.canal.client.adapter.es.support.transformer;

import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.model.TransformContext;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/14
 * @Version1.0
 */
public class TransformerFactory implements Transformer {
    private static TransformerFactory transformerFactory = new TransformerFactory();

    private static Map<String, Transformer> transformerMap = new HashMap<>();

    @Override
    public void transform(ESData esData, TransformContext context) {
        Transformer transformer = transformerMap.get(context.getEsMapping().getConfigFileName());
        if (transformer != null) transformer.transform(esData, context);
    }

    public static TransformerFactory getInstance() {
        return transformerFactory;
    }


    public static void init() throws Exception {
        for (Class<?> cls : ESSyncUtil.getClasses(Transformer.class.getPackage().getName())) {
            Service annotation = cls.getAnnotation(Service.class);
            if (annotation != null) {
                Constructor<?> constructor = cls.getConstructor();
                Transformer transformer = (Transformer) constructor.newInstance();
                transformerMap.put(cls.getSimpleName().toLowerCase(), transformer);
            }
        }
    }
}
