package com.alibaba.otter.canal.client.adapter.es.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.DataHandler;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.DataHandlerFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;

/**
 * ES 同步 Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncService {

    private static Logger logger = LoggerFactory.getLogger(ESSyncService.class);

    private ESTemplate esTemplate;

    public ESSyncService(ESTemplate esTemplate) {
        this.esTemplate = esTemplate;
    }

    public void sync(Collection<ESSyncConfig> esSyncConfigs, Dml dml) {
        long begin = System.currentTimeMillis();
        if (esSyncConfigs != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Destination: {}, database:{}, table:{}, type:{}, affected index count: {}",
                        dml.getDestination(),
                        dml.getDatabase(),
                        dml.getTable(),
                        dml.getType(),
                        esSyncConfigs.size());
            }
            //串行
            for (ESSyncConfig config : esSyncConfigs) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Prepared to sync index: {}, destination: {}",
                            config.getEsMapping().get_index(),
                            dml.getDestination());
                }
                this.sync(config, dml);
                if (logger.isTraceEnabled()) {
                    logger.trace("Sync completed: {}, destination: {}",
                            config.getEsMapping().get_index(),
                            dml.getDestination());
                }
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Sync elapsed time: {} ms, affected indexes count：{}, destination: {}",
                        (System.currentTimeMillis() - begin),
                        esSyncConfigs.size(),
                        dml.getDestination());
            }
            if (logger.isDebugEnabled()) {
                StringBuilder configIndexes = new StringBuilder();
                esSyncConfigs
                        .forEach(esSyncConfig -> configIndexes.append(esSyncConfig.getEsMapping().get_index()).append(" "));
                logger.debug("DML: {} \nAffected indexes: {}",
                        JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue),
                        configIndexes.toString());
            }
        }
    }

    public void sync(ESSyncConfig config, Dml dml) {
        try {
            // 如果是按时间戳定时更新则返回
            if (config.getEsMapping().isSyncByTimestamp()) {
                return;
            }

            long begin = System.currentTimeMillis();

            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase(OpTypeEnum.INSERT.value)) {
                insert(config, dml);
            } else if (type != null && type.equalsIgnoreCase(OpTypeEnum.UPDATE.value)) {
                update(config, dml);
            } else if (type != null && type.equalsIgnoreCase(OpTypeEnum.DELETE.value)) {
                delete(config, dml);
            } else {
                return;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("Sync elapsed time: {} ms,destination: {}, es index: {}",
                        (System.currentTimeMillis() - begin),
                        dml.getDestination(),
                        config.getEsMapping().get_index());
            }
        } catch (Throwable e) {
            logger.error("sync error, es index: {}, DML : {}", config.getEsMapping().get_index(), dml);
            throw new RuntimeException(e);
        }
    }


    /**
     * 插入操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private void insert(ESSyncConfig config, Dml dml) {
        update(config, dml, OpTypeEnum.INSERT);
    }

    /**
     * 更新操作dml  同插入操作
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private void update(ESSyncConfig config, Dml dml) {
        update(config, dml, OpTypeEnum.UPDATE);
    }

    //TODO 更新时如果没有配置中需要的字段则丢弃该请求

    private void update(ESSyncConfig config, Dml dml, OpTypeEnum opTypeEnum) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }

        ESSyncConfig.ESMapping mapping = config.getEsMapping();

        for (Map<String, Object> sourceData : dataList) {
            if (sourceData == null || sourceData.isEmpty()) continue;

            //数据转换
            DataHandler dataHandler = DataHandlerFactory.getDataHandler(mapping.getConfigFileName());
            Map<String, Object> esFieldData = dataHandler.dispose(config, sourceData, opTypeEnum);

            //取得主键值
            Object idVal = esFieldData.remove(mapping.get_id());
            if (idVal == null) throw new RuntimeException("idVal can not be null");
            if (logger.isTraceEnabled()) {
                logger.trace("update to es index, destination:{}, table: {}, index: {}, id: {}",
                        config.getDestination(),
                        dml.getTable(),
                        mapping.get_index(),
                        idVal);
            }

            esTemplate.update(mapping, idVal, esFieldData, opTypeEnum);
        }
    }

    /**
     * 删除操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    private void delete(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        ESSyncConfig.ESMapping mapping = config.getEsMapping();

        if (dataList == null || dataList.isEmpty()) return;

        for (Map<String, Object> sourceData : dataList) {
            if (sourceData == null || sourceData.isEmpty()) continue;

            Map<String, Object> esFieldData = new LinkedHashMap<>();

            Object idVal = ESSyncUtil.dataMapping(sourceData, mapping.getProperties().get(mapping.get_id()), mapping.get_id(), OpTypeEnum.DELETE);

            if (idVal == null) throw new RuntimeException("idVal can not be null");

            //带flattened字段的删除操作
            if (!mapping.isMain()) {
                String flattenedField = mapping.getFlattenedField();
                if (flattenedField != null) {
                    Object subField = sourceData.get(ESSyncUtil.strToArray(mapping.getProperties().get(flattenedField).getColumn())[0]);
                    if (subField != null) {
                        Map<String, Object> params = new HashMap<>();
                        params.put("field", flattenedField);
                        params.put("subField", subField);
                        Script script = new Script(ScriptType.STORED, null, "flattened-remove", params);
                        esTemplate.scriptUpdate(mapping, idVal.toString(), script, OpTypeEnum.DELETE);
                        return;
                    }
                }
            }

            //删除操作
            esTemplate.delete(mapping, idVal.toString(), esFieldData, OpTypeEnum.DELETE);
        }
    }

    /**
     * 提交批次
     */
    public void commit() {
        esTemplate.commit();
    }
}
