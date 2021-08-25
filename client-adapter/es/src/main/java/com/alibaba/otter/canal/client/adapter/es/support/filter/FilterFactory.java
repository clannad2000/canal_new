package com.alibaba.otter.canal.client.adapter.es.support.filter;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.extractor.AbstractExtractor;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.DataHandler;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
import com.google.common.collect.TreeMultimap;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.Comparator;
import java.util.NavigableSet;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/15
 * @Version1.0
 */
public class FilterFactory implements Filter {
    private static FilterFactory filterFactory = new FilterFactory();
    private static TreeMultimap<String, AbstractFilter> extractorMap = TreeMultimap.create((o1, o2) -> o1.equals(o2) ? 0 : 1, Comparator.comparingInt(o -> o.order));


    public static void init() throws Exception {
        for (Class<?> cls : ESSyncUtil.getClasses(Filter.class.getPackage().getName())) {
            Service annotation = cls.getAnnotation(Service.class);
            if (annotation != null) {
                Constructor<?> constructor = cls.getConstructor();
                AbstractFilter filter = (AbstractFilter) constructor.newInstance();
                extractorMap.put(cls.getSimpleName().toLowerCase(), filter);
            }
        }
    }


    @Override
    public boolean filter(ESSyncConfig config, FlatDml flatDml) {
        NavigableSet<AbstractFilter> filters = extractorMap.get(config.getEsMapping().getConfigFileName());
        for (Filter filter : filters) {
            if (!filter.filter(config, flatDml)) return false;
        }
        return true;
    }

    public static FilterFactory getInstance() {
        return filterFactory;
    }
}
