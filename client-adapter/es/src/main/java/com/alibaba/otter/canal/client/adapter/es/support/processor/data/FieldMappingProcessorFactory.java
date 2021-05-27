package com.alibaba.otter.canal.client.adapter.es.support.processor.data;

import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.ArrayProcessor;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.ConcatProcessor;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.FlattenedProcessor;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.GeoPointProcessor;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.HHmmDateProcessor;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl.SqlForListProcessor;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public class FieldMappingProcessorFactory {
    private static Map<String, FieldMappingProcessor> map = new HashMap<>();

    static {
        map.put("geo_point", new GeoPointProcessor());
        map.put("array", new ArrayProcessor());
        map.put("concat", new ConcatProcessor());
        map.put("HHmmDate", new HHmmDateProcessor());
        map.put("flattened", new FlattenedProcessor());
        map.put("sqlForList", new SqlForListProcessor());
    }

    public static FieldMappingProcessor getInstance(String name) {
        FieldMappingProcessor mappingProcessor = map.get(name);
        if (mappingProcessor == null) throw new RuntimeException("Not found " + name);
        return mappingProcessor;
    }
}
