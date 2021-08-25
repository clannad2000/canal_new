package com.alibaba.otter.canal.client.adapter.es.support;

import java.util.Map;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;

public interface ESTemplate {

    /**
     * 根据主键更新数据
     *
     * @param mapping     配置对象
     * @param pkVal       主键值
     * @param esFieldData 数据Map
     * @param opTypeEnum
     */
    void update(ESData esData);


    /**
     * 脚本更新
     *
     * @param mapping 配置对象
     * @param pkVal   主键值
     * @param script  es脚本id
     * @param delete
     */
    void scriptUpdate(ESData esData);


    /**
     * update by query for mysql
     *
     * @param config      配置对象
     * @param paramsTmp   sql查询条件
     * @param esFieldData 数据Map
     */
    void updateByQueryForSql(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData);


    /**
     * update by query for es
     *
     * @param mapping     配置对象
     * @param esFieldData 数据Map
     */
    void updateByQuery(ESData esData);


    /**
     * 通过主键删除数据
     * @param mapping 配置对象
     * @param idVal   主键值
     */
    void delete(ESData esData);

    /**
     * 提交批次
     */
    void commit();

}
