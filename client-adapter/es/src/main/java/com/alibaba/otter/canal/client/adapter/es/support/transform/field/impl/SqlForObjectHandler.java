package com.alibaba.otter.canal.client.adapter.es.support.transform.field.impl;


import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.transform.field.FieldMappingHandler;
import com.alibaba.otter.canal.client.adapter.support.SqlUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/24
 * @Version1.0
 */
public class SqlForObjectHandler implements FieldMappingHandler {

    @Override
    public Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, OpTypeEnum opTypeEnum) {
        List<Object> params = new ArrayList<>();
        ESSyncUtil.strToList(fieldMapping.getParams())
                .forEach(columnName -> params.add(sourceData.get(columnName)));
        return SqlUtil.queryForObject(fieldMapping.getDataSourceKey(), fieldMapping.getSql(), params);
    }
}
