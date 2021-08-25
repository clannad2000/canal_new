package com.alibaba.otter.canal.client.adapter.es.support.transform.data;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;

import java.util.Map;

/**
 * 数据处理器接口
 * @Description
 * @Author 黄念
 * @Date 2021/6/11
 * @Version1.0
 */
public interface DataHandler {

    /** 前置处理器
     * @param sourceData 源数据
     * @param mapping    映射配置
     * @param opTypeEnum 操作类型
     * @return sourceData
     * @author 黄念
     * @date 2021/6/11 11:03
     */
    Map<String, Object> preDispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping mapping, OpTypeEnum opTypeEnum);


    /** 字段映射
     * @param sourceData 源数据
     * @param mapping    映射配置
     * @param opTypeEnum 操作类型
     * @return esFieldData
     * @author 黄念
     * @date 2021/6/11 11:03
     */
    Map<String, Object> fieldMapping(Map<String, Object> sourceData, ESSyncConfig.ESMapping mapping, OpTypeEnum opTypeEnum);


    /** 后置处理
     * @param esSyncConfig 配置
     * @param sourceData   源数据
     * @param esFieldData  es数据
     * @param opTypeEnum   操作类型
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author 黄念
     * @date 2021/6/11 11:03
     */
    ESData postDispose(ESSyncConfig esSyncConfig, Map<String, Object> sourceData, Map<String, Object> esFieldData, OpTypeEnum opTypeEnum);


    /** 数据处理
     * @param esSyncConfig 配置
     * @param sourceData   源数据
     * @param opTypeEnum   操作类型
     * @return esFieldData
     * @author 黄念
     * @date 2021/6/11 11:03
     */
    ESData dispose(ESSyncConfig esSyncConfig, Map<String, Object> sourceData, OpTypeEnum opTypeEnum);
}
