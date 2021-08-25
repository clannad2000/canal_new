package com.alibaba.otter.canal.client.adapter.es.support.extractor;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.extractor.processor.impl.tyre.TyredbSkuSProductSku;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.DataHandler;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.TreeMultimap;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/14
 * @Version1.0
 */
public class ExtractorFactory implements Extractor {
    private static ExtractorFactory extractorFactory = new ExtractorFactory();

    private static TreeMultimap<String, AbstractExtractor> extractorMap = TreeMultimap.create((o1, o2) -> o1.equals(o2) ? 0 : 1, Comparator.comparing(o -> o.order));


    @Override
    public void extract(FlatDml flatDml, ExtractorContext context) {
        NavigableSet<AbstractExtractor> extractors = extractorMap.get(context.getConfig().getEsMapping().getConfigFileName());
        extractors.forEach(extractor -> extractor.extract(flatDml, context));
    }

    public static ExtractorFactory getInstance() {
        return extractorFactory;
    }

    public static void init() throws Exception {
        for (Class<?> cls : ESSyncUtil.getClasses(Extractor.class.getPackage().getName())) {
            Service annotation = cls.getAnnotation(Service.class);
            if (annotation != null) {
                Constructor<?> constructor = cls.getConstructor();
                AbstractExtractor extractor = (AbstractExtractor) constructor.newInstance();
                extractorMap.put(cls.getSimpleName().toLowerCase(), extractor);
            }
        }
    }

}
