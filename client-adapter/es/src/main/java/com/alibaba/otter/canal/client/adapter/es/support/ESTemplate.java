package com.alibaba.otter.canal.client.adapter.es.support;

import java.util.Map;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;

public interface ESTemplate {

    /**
     * 插入数据
     *
     * @param mapping 配置对象
     * @param pkVal 主键值
     * @param esFieldData 数据Map
     */
    void insert(ESSyncConfig.ESMapping mapping, Object pkVal, Map<String, Object> esFieldData);


    /**
     * 根据主键更新数据
     *
     * @param mapping 配置对象
     * @param pkVal 主键值
     * @param esFieldData 数据Map
     */
    void update(ESSyncConfig.ESMapping mapping, Object pkVal, Map<String, Object> esFieldData);


    /**
     * update by query
     *
     * @param config 配置对象
     * @param paramsTmp sql查询条件
     * @param esFieldData 数据Map
     */
    void updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData);


    /**
     * 通过主键删除数据
     *
     * @param mapping 配置对象
     * @param idVal 主键值
     */
    void delete(ESSyncConfig.ESMapping mapping, Object idVal);


    /**
     * 提交批次
     */
    void commit();
}
