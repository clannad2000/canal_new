package com.alibaba.otter.canal.client.adapter.es.support.transform.field;

import com.alibaba.otter.canal.client.adapter.es.support.transform.field.impl.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public class FieldMappingHandlerFactory {
    private static Map<String, FieldMappingHandler> map = new HashMap<>();

    static {
        map.put("geo_point", new GeoPointHandler());
        map.put("array", new ArrayHandler());
        map.put("concat", new ConcatHandler());
        map.put("HHmmDate", new HHmmDateHandler());
        map.put("flattened", new FlattenedHandler());
        map.put("sqlForList", new SqlForListHandler());
        map.put("sqlForMapList", new SqlForMapListHandler());
        map.put("sqlForObject", new SqlForObjectHandler());
    }

    public static FieldMappingHandler getInstance(String name) {
        FieldMappingHandler mappingProcessor = map.get(name);
        if (mappingProcessor == null) throw new RuntimeException("Not found " + name);
        return mappingProcessor;
    }
}
