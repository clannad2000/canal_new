package com.alibaba.otter.canal.client.adapter.es.support.processor.data.impl;



import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OperationEnum;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.FieldMappingProcessor;
import com.alibaba.otter.canal.client.adapter.support.Util;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/24
 * @Version1.0
 */
public class SqlForListProcessor implements FieldMappingProcessor {

    @Override
    public Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, OperationEnum operationEnum) {
        List<Object> params = new ArrayList<>();
        List<String> columns = ESSyncUtil.strToList(fieldMapping.getColumn());

        columns.forEach(columnName -> params.add(sourceData.get(columnName)));

        return Util.executeSqlForList(fieldMapping.getDataSourceKey(), fieldMapping.getSql(), params);
    }
}
