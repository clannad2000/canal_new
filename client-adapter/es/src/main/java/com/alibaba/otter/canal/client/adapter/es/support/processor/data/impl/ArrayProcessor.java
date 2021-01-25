package com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.DataMappingProcessor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/29
 * @Version1.0
 */
public class ArrayProcessor implements DataMappingProcessor {
    @Override
    public Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping) {

        List<String> dataFiled = ESSyncUtil.strToList(fieldMapping.getColumn());
        List<String> list = dataFiled.stream()
                .map(sourceData::get)
                .filter(Objects::nonNull)
                .flatMap(ele -> ESSyncUtil.strToList(ele.toString()).stream())
                .collect(Collectors.toList());
        return list.size() == 0 ? null : list;
    }
}
