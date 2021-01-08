package com.alibaba.otter.canal.client.adapter.es.support.handler.impl;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.handler.DataMappingHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public class GeoPointHandler implements DataMappingHandler {

    @Override
    public Object handle(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping) {
        String[] pointFiled = ESSyncUtil.strToArray(fieldMapping.getColumn());
        Map<String, Double> location = new HashMap<>();
        Object lat = sourceData.get(pointFiled[0].trim());
        Object lon = sourceData.get(pointFiled[1].trim());
        if (lat == null || lon == null) return null;
        location.put("lat", Double.valueOf(lat.toString()));
        location.put("lon", Double.valueOf(lon.toString()));
        return location;
    }
}
