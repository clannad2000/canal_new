package com.alibaba.otter.canal.client.adapter.es.support.handler;

import com.alibaba.otter.canal.client.adapter.es.support.handler.impl.ArrayHandler;
import com.alibaba.otter.canal.client.adapter.es.support.handler.impl.ConcatHandler;
import com.alibaba.otter.canal.client.adapter.es.support.handler.impl.GeoPointHandler;
import com.alibaba.otter.canal.client.adapter.es.support.handler.impl.HHmmDateHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public class DataMappingHandlerFactory {
    private static Map<String, DataMappingHandler> map = new HashMap<>();

    static {
        map.put("geo_point", new GeoPointHandler());
        map.put("array", new ArrayHandler());
        map.put("concat", new ConcatHandler());
        map.put("HHmmDate", new HHmmDateHandler());
    }

    public static DataMappingHandler getInstance(String name) {
        return map.get(name);
    }
}
