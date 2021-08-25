package com.alibaba.otter.canal.client.adapter.es.support.extractor;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;

import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/13
 * @Version1.0
 */
public interface Extractor {

    /**
     * 数据装配
     */
    void extract(FlatDml flatDml, ExtractorContext context);
}
