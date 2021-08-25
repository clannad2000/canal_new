package com.alibaba.otter.canal.client.adapter.es.support.transformer.processor;

import com.alibaba.otter.canal.client.adapter.es.support.emun.ParamsSrcType;
import com.alibaba.otter.canal.client.adapter.es.support.mapper.MapperFactory;
import com.alibaba.otter.canal.client.adapter.es.support.model.MapperContext;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.model.TransformContext;
import com.alibaba.otter.canal.client.adapter.es.support.transformer.AbstractTransformer;

import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/14
 * @Version1.0
 */
public abstract class AbstractProcessorTransformer extends AbstractTransformer {

    public void transform(ESData esData, TransformContext context) {
        preprocess(esData, context);
        if (OpTypeEnum.INSERT == esData.getSrcOpType()) insert(esData, context);
        if (OpTypeEnum.UPDATE == esData.getSrcOpType()) update(esData, context);
        if (OpTypeEnum.DELETE == esData.getSrcOpType()) delete(esData, context);
        if (esData.getParamsSrc() != ParamsSrcType.PARAMS)
            MapperFactory.getInstance().mapping(esData, MapperContext.builder().esMapping(context.getEsMapping()).build());
    }

    protected abstract void preprocess(ESData esData, TransformContext context);

    protected abstract void insert(ESData esData, TransformContext context);

    protected abstract void update(ESData esData, TransformContext context);

    protected abstract void delete(ESData esData, TransformContext context);

}
