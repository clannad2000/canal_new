package com.alibaba.otter.canal.client.adapter.es.service;

import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.load.Loader;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/22
 * @Version1.0
 */
public class EtlESSyncService extends AbstractESSyncService {

    public EtlESSyncService(ESTemplate esTemplate, Loader loader) {
        super(esTemplate, loader);
    }

    @Override
    public void sync(ExtractorContext extractorContext) {
        sync2(extractorContext);
    }
}
