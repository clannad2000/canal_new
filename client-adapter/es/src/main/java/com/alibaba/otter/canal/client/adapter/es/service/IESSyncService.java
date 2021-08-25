package com.alibaba.otter.canal.client.adapter.es.service;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/22
 * @Version1.0
 */
public interface IESSyncService {
    void sync(ExtractorContext extractorContext);
    void commit();
}
