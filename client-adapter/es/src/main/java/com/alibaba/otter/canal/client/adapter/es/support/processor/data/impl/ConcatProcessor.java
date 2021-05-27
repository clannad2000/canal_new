package com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OperationEnum;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.FieldMappingProcessor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/29
 * @Version1.0
 * str1="武汉市", str2="汉阳区" -> str="武汉市汉阳区"
 */
public class ConcatProcessor implements FieldMappingProcessor {
    @Override
    public Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, OperationEnum operationEnum) {
        List<String> dataFiled = ESSyncUtil.strToList(fieldMapping.getColumn());
        String s = dataFiled.stream().map(key -> sourceData.get(key).toString()).filter(Objects::nonNull).collect(Collectors.joining());
        return s.equals("") ? null : s;
    }
}
