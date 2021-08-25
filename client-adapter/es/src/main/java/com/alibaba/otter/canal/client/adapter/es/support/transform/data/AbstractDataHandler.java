package com.alibaba.otter.canal.client.adapter.es.support.transform.data;


import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/6/11
 * @Version1.0
 */
public abstract class AbstractDataHandler implements DataHandler {
    Logger logger = LoggerFactory.getLogger(AbstractDataHandler.class);
    //protected JdbcTemplate jdbcTemplate = new JdbcTemplate();


    /**
     * 前置处理器
     *
     * @param sourceData 源数据
     * @param mapping    映射配置
     * @param opTypeEnum 操作类型
     * @return sourceData
     * @author 黄念
     * @date 2021/6/11 11:03
     */
    @Override
    public Map<String, Object> preDispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping mapping, OpTypeEnum opTypeEnum) {
        return sourceData;
    }


    /**
     * 字段映射
     *
     * @param sourceData 源数据
     * @param mapping    映射配置
     * @param opTypeEnum 操作类型
     * @return esFieldData
     * @author 黄念
     * @date 2021/6/11 11:03
     */
    @Override
    public Map<String, Object> fieldMapping(Map<String, Object> sourceData, ESSyncConfig.ESMapping mapping, OpTypeEnum opTypeEnum) {
            Map<String, Object> esFieldData = new LinkedHashMap<>();
            try {
                mapping.getProperties().forEach((esFieldName, fieldMapping) -> {
                    Object value = ESSyncUtil.dataMapping(sourceData, fieldMapping, esFieldName, opTypeEnum);
                    esFieldData.put(esFieldName, value);
                });
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
            return esFieldData;
    }


    /**
     * 后置处理
     *
     * @param esSyncConfig 配置
     * @param sourceData   源数据
     * @param esFieldData  es数据
     * @param opTypeEnum   操作类型
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author 黄念
     * @date 2021/6/11 11:03
     */
    @Override
    public ESData postDispose(ESSyncConfig esSyncConfig, Map<String, Object> sourceData, Map<String, Object> esFieldData, OpTypeEnum opTypeEnum) {
        ESSyncConfig.ESMapping esMapping = esSyncConfig.getEsMapping();
        return ESData.builder()
                .srcOpType(opTypeEnum)
                .index(esMapping.get_index())
                .esFieldData(esFieldData)
                .upsert(esSyncConfig.getEsMapping().isUpsert())
                .build();
    }


    /**
     * 数据处理
     *
     * @param esSyncConfig 配置
     * @param sourceData   源数据
     * @param opTypeEnum   操作类型
     * @return esFieldData
     * @author 黄念
     * @date 2021/6/11 11:03
     */
    @Override
    public ESData dispose(ESSyncConfig esSyncConfig, Map<String, Object> sourceData, OpTypeEnum opTypeEnum) {
        ESSyncConfig.ESMapping mapping = esSyncConfig.getEsMapping();
        try {

            //前置处理
            preDispose(sourceData, mapping, opTypeEnum);

            //字段映射
            Map<String, Object> esFieldData = fieldMapping(sourceData, mapping, opTypeEnum);

            //后置处理
            return postDispose(esSyncConfig, sourceData, esFieldData, opTypeEnum);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
