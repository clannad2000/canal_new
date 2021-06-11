package com.alibaba.otter.canal.client.adapter.es.support.transform.field.impl;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.transform.field.FieldMappingHandler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/29
 * @Version1.0 str1="武汉市", str2="汉阳区" -> str="武汉市汉阳区"
 */
public class ConcatHandler implements FieldMappingHandler {
    @Override
    public Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, OpTypeEnum opTypeEnum) {
        List<String> dataFiled = ESSyncUtil.strToList(fieldMapping.getColumn());
        String s = dataFiled.stream().map(key -> sourceData.get(key) != null ? sourceData.get(key).toString() : null).filter(Objects::nonNull).collect(Collectors.joining());
        return s.equals("") ? null : s;
    }
}
