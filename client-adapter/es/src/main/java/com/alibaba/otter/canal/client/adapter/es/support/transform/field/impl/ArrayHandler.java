package com.alibaba.otter.canal.client.adapter.es.support.transform.field.impl;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.transform.field.FieldMappingHandler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/29
 * @Version1.0
 * str1="1,2,3,4", str2="5,6,7,8" ... -> list={1,2,3,4,5,6,7,8}
 */
public class ArrayHandler implements FieldMappingHandler {
    @Override
    public Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, OpTypeEnum opTypeEnum) {

        List<String> dataFiled = ESSyncUtil.strToList(fieldMapping.getColumn());
        List<String> list = dataFiled.stream()
                .map(sourceData::get)
                .filter(Objects::nonNull)
                .flatMap(ele -> ESSyncUtil.strToList(ele.toString()).stream())
                .collect(Collectors.toList());
        return list.size() == 0 ? null : list;
    }
}
