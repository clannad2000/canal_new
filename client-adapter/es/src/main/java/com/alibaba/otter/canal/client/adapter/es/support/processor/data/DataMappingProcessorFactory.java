package com.alibaba.otter.canal.client.adapter.es.support.processor.data;

import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.ArrayProcessor;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.ConcatProcessor;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.GeoPointProcessor;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.HHmmDateProcessor;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public class DataMappingProcessorFactory {
    private static Map<String, DataMappingProcessor> map = new HashMap<>();

    static {
        map.put("geo_point", new GeoPointProcessor());
        map.put("array", new ArrayProcessor());
        map.put("concat", new ConcatProcessor());
        map.put("HHmmDate", new HHmmDateProcessor());
    }

    public static DataMappingProcessor getInstance(String name) {
        return map.get(name);
    }
}
