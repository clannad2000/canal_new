package com.alibaba.otter.canal.client.adapter.es.support.extractor.processor;

import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.extractor.AbstractExtractor;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/14
 * @Version1.0
 */
public abstract class AbstractProcessorExtractor extends AbstractExtractor
{

    @Override
    public final void extract(FlatDml flatDml, ExtractorContext context)
    {
        preprocess(flatDml, context);
        if (OpTypeEnum.INSERT == flatDml.getType()) insert(flatDml, context);
        else if (OpTypeEnum.UPDATE == flatDml.getType()) update(flatDml, context);
        else if (OpTypeEnum.DELETE == flatDml.getType()) delete(flatDml, context);
    }

    protected abstract void preprocess(FlatDml flatDml, ExtractorContext context);

    protected abstract void insert(FlatDml flatDml, ExtractorContext context);

    protected abstract void update(FlatDml flatDml, ExtractorContext context);

    protected abstract void delete(FlatDml flatDml, ExtractorContext context);
}
