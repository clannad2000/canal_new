package com.alibaba.otter.canal.client.adapter.es.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.EnumUtils;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.extractor.ExtractorFactory;
import com.alibaba.otter.canal.client.adapter.es.support.filter.FilterFactory;
import com.alibaba.otter.canal.client.adapter.es.support.load.Loader;
import com.alibaba.otter.canal.client.adapter.es.support.mapper.MapperFactory;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;
import com.alibaba.otter.canal.client.adapter.es.support.model.MapperContext;
import com.alibaba.otter.canal.client.adapter.es.support.model.TransformContext;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.DataHandler;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.DataHandlerFactory;
import com.alibaba.otter.canal.client.adapter.es.support.transformer.TransformerFactory;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
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

    private Loader esLoader;

    public ESSyncService(ESTemplate esTemplate, Loader loader) {
        this.esTemplate = esTemplate;
        this.esLoader = loader;
    }

    public void sync(Collection<ESSyncConfig> esSyncConfigs, List<Dml> dmls, int index) {
        Dml dml = dmls.get(index);
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
            //并行,怎么提交
            //同一条数据各个分组的消费进度可能不一致,必须所有的分组都消费完成才可以提交
            for (ESSyncConfig config : esSyncConfigs) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Prepared to sync index: {}, destination: {}",
                            config.getEsMapping().get_index(),
                            dml.getDestination());
                }
                //this.sync(config, dml);
                syncTest(config, dmls, index);
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

    public void syncTest(ESSyncConfig config, List<Dml> dmls, int index) {
        ESSyncConfig.ESMapping esMapping = config.getEsMapping();
        //查询->过滤->提取->转换->映射->载入
        //dml顺序修改后是否会对其他的任务产生影响
        //解决方案2: 回查, 写专用的回查接口
        List<FlatDml> flatDmlList = buildFlatDmlList(dmls.get(index));
        ExtractorContext extractorContext = ExtractorContext.builder()
                .config(config)
                .dmls(dmls)
                .index(index)
                .build();

        for (FlatDml flatDml : flatDmlList) {
            //过滤
            if (!FilterFactory.getInstance().filter(config, flatDml)) continue;

            //提取
            ExtractorFactory.getInstance().extract(flatDml, extractorContext);
            ESData esData = ESData.builder()
                    .index(esMapping.get_index())
                    .upsert(esMapping.isUpsert())
                    .srcOpType(flatDml.getType())
                    .flatDml(flatDml)
                    .esFieldData(flatDml.getData())
                    .build();

            //转换
            TransformContext transformContext = TransformContext.builder().esMapping(config.getEsMapping()).build();
            TransformerFactory.getInstance().transform(esData, transformContext);
           // esData.setIdVal(esData.getEsFieldData().get(esMapping.get_id()).toString());

            //映射
            MapperFactory.getInstance().mapping(esData, MapperContext.builder().esMapping(config.getEsMapping()).build());

            //载入
            esLoader.load(esData, config.getEsMapping());
        }
    }


    public List<FlatDml> buildFlatDmlList(Dml dml) {
        List<FlatDml> list = new ArrayList<>(dml.getData().size());
        for (int i = 0; i < dml.getData().size(); i++) {
            FlatDml flatDml = FlatDml.builder()
                    .destination(dml.getDestination())
                    .groupId(dml.getGroupId())
                    .database(dml.getDatabase())
                    .table(dml.getTable())
                    .isDdl(dml.getIsDdl())
                    .es(dml.getEs())
                    .ts(dml.getTs())
                    .pkNames(dml.getPkNames())
                    .sql(dml.getSql())
                    .type(EnumUtils.getInstance(OpTypeEnum.class, dml.getType().toLowerCase()))
                    .data(dml.getData().get(i))
                    .old(dml.getOld() != null ? dml.getOld().get(i) : null)
                    .build();
            list.add(flatDml);
        }
        return list;
    }


    public void sync(ESSyncConfig config, Dml dml) {
        try {
            // 如果是按时间戳定时更新则返回
            if (config.getEsMapping().isSyncByTimestamp()) {
                return;
            }

            long begin = System.currentTimeMillis();

            String type = dml.getType();
            if (config.getUpdateMode()) {
                update(config, dml);
            } else if (type != null && type.equalsIgnoreCase(OpTypeEnum.INSERT.value)) {
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
            ESData esData = dataHandler.dispose(config, sourceData, opTypeEnum);

            //取得主键值
            //Object idVal = esData.getEsFieldData().remove(mapping.get_id());
            esData.setIdVal(esData.getIdVal());
            if (esData.getIdVal() == null) throw new RuntimeException("idVal can not be null");
            if (logger.isTraceEnabled()) {
                logger.trace("update to es index, destination:{}, table: {}, index: {}, id: {}",
                        config.getDestination(),
                        dml.getTable(),
                        mapping.get_index(),
                        esData.getIdVal());
            }

            //esTemplate.update(mapping, idVal, esFieldData, opTypeEnum);
            //esLoader.load(mapping, esData);
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
                        //Script script = new Script(ScriptType.STORED, null, "flattened-remove", params);
                        //esTemplate.scriptUpdate(mapping, idVal.toString(), script, OpTypeEnum.DELETE);
//                        esLoader.load(mapping, ESData.builder()
//                                .idVal(idVal.toString())
//                                .upsert(mapping.isUpsert())
//                                .script("flattened-remove")
//                                .params(params)
//                                .opTypeEnum(OpTypeEnum.UPDATE_BY_QUERY)
//                                .build());
                        return;
                    }
                }
            }

            //删除操作
            // esTemplate.delete(mapping, idVal.toString(), esFieldData, OpTypeEnum.DELETE);
//            esLoader.load(mapping, ESData.builder()
//                    .idVal(idVal.toString())
//                    .upsert(mapping.isUpsert())
//                    .esFieldData(esFieldData)
//                    .opTypeEnum(OpTypeEnum.DELETE)
//                    .build());
        }
    }

    /**
     * 提交批次
     */
    public void commit() {
        esTemplate.commit();
    }


    //POST _scripts/flattened-remove
    //{
    //  "script": {
    //    "lang": "painless",
    //    "source": "ctx._source.get(params.field).remove(params.subField)"
    //  }
    //}

}
