package com.alibaba.otter.canal.client.adapter.es.support.mapper;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.emun.ParamsSrcType;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.model.MapperContext;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/19
 * @Version1.0
 */
public class DefaultMapper extends AbstractMapper {

    @Override
    public void mapping(ESData esData, MapperContext context) {

        Map<String, Object> esFieldData = new LinkedHashMap<>();

        if (esData.getDstOpType() == OpTypeEnum.DELETE) {
            ESSyncConfig.ESMapping.FieldMapping fieldMapping = context.getEsMapping().getProperties().get(context.getEsMapping().get_id());
            Object value = ESSyncUtil.dataMapping(esData.getFlatDml().getData(), fieldMapping, context.getEsMapping().get_id(), esData.getSrcOpType());

            context.getEsMapping().getProperties().keySet().forEach(key -> esFieldData.put(key, null));
            esFieldData.remove(context.getEsMapping().get_id());

            esData.setIdVal(value.toString());
            esData.setEsFieldData(esFieldData);
        }

        try {
            context.getEsMapping().getProperties().forEach((esFieldName, fieldMapping) -> {
                Object value = ESSyncUtil.dataMapping(esData.getFlatDml().getData(), fieldMapping, esFieldName, esData.getSrcOpType());
                esFieldData.put(esFieldName, value);
            });
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        esData.setIdVal(esData.getEsFieldData().get(context.getEsMapping().get_id()).toString());
        esData.setEsFieldData(esFieldData);
    }
}
