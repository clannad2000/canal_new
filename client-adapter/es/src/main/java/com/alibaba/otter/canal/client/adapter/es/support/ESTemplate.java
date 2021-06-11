package com.alibaba.otter.canal.client.adapter.es.support;

import java.util.Map;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OpTypeEnum;
import org.elasticsearch.script.Script;

public interface ESTemplate {

    /**
     * 根据主键更新数据
     *  @param mapping     配置对象
     * @param pkVal       主键值
     * @param esFieldData 数据Map
     * @param opTypeEnum
     */
    void update(ESSyncConfig.ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, OpTypeEnum opTypeEnum);


    /**
     * 脚本更新
     *  @param mapping 配置对象
     * @param pkVal   主键值
     * @param script  es脚本id
     * @param delete
     */
    void scriptUpdate(ESSyncConfig.ESMapping mapping, String pkVal, Script script, OpTypeEnum delete);


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
    void updateByQueryForES(ESSyncConfig.ESMapping mapping, Map<String, Object> esFieldData);


    /**
     * 通过主键删除数据
     * @param mapping 配置对象
     * @param idVal   主键值
     */
    void delete(ESSyncConfig.ESMapping mapping, String idVal, Map<String, Object> esFieldData, OpTypeEnum opTypeEnum);

    /**
     * 提交批次
     */
    void commit();

}
