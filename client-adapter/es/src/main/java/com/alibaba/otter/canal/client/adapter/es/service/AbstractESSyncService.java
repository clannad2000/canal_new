package com.alibaba.otter.canal.client.adapter.es.service;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.extractor.ExtractorFactory;
import com.alibaba.otter.canal.client.adapter.es.support.filter.FilterFactory;
import com.alibaba.otter.canal.client.adapter.es.support.load.Loader;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;
import com.alibaba.otter.canal.client.adapter.es.support.model.TransformContext;
import com.alibaba.otter.canal.client.adapter.es.support.transformer.TransformerFactory;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/22
 * @Version1.0
 */
public abstract class AbstractESSyncService implements IESSyncService {

    protected static Logger logger = LoggerFactory.getLogger(AbstractESSyncService.class);

    protected ESTemplate esTemplate;

    protected Loader esLoader;

    public AbstractESSyncService(ESTemplate esTemplate, Loader loader) {
        this.esTemplate = esTemplate;
        this.esLoader = loader;
    }


    public void sync2(ExtractorContext extractorContext) {
        ESSyncConfig config = extractorContext.getConfig();
        ESSyncConfig.ESMapping esMapping = config.getEsMapping();
        FlatDml flatDml = extractorContext.getFlatDml();
        //过滤
        if (!FilterFactory.getInstance().filter(extractorContext.getConfig(), flatDml)) return;

        //提取
        ExtractorFactory.getInstance().extract(flatDml, extractorContext);

        //转换
        ESData esData = ESData.builder()
                .index(esMapping.get_index())
                .upsert(esMapping.isUpsert())
                .srcOpType(flatDml.getType())
                .dstOpType(getDstOpType(esMapping))
                .flatDml(flatDml)
                .esFieldData(flatDml.getData())
                .build();

        TransformContext transformContext = TransformContext.builder().esMapping(config.getEsMapping()).build();
        TransformerFactory.getInstance().transform(esData, transformContext);

        //载入
        esLoader.load(esData, config.getEsMapping());
    }

    private OpTypeEnum getDstOpType(ESSyncConfig.ESMapping esMapping) {
        if (esMapping.getUpdateByQuery() != null) return OpTypeEnum.UPDATE_BY_QUERY;
        if (esMapping.getScriptedUpdate() != null) return OpTypeEnum.SCRIPTED_UPDATE;
        return null;
    }

    /**
     * 提交批次
     */
    @Override
    public void commit() {
        esTemplate.commit();
    }

}
