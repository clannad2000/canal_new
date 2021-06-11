package com.alibaba.otter.canal.client.adapter.es.support.transform.field.impl;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.transform.field.FieldMappingHandler;

import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/29
 * @Version1.0
 * str="8:30" -> long=8*60*60*1000+30*60*1000
 */
public class HHmmDateHandler implements FieldMappingHandler {

    @Override
    public Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, OpTypeEnum opTypeEnum) {
        String column = fieldMapping.getColumn();
        List<Integer> list = ESSyncUtil.strToIntList(sourceData.get(column)!=null?sourceData.get(column).toString():null, ":");
        if (list.isEmpty()) return null;
        return list.get(0) * 60 * 60 * 1000 + list.get(1) * 60 * 1000;
    }
}
