package com.alibaba.otter.canal.client.adapter.es.support.filter;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/14
 * @Version1.0
 */
public interface Filter {
    boolean filter(ESSyncConfig config, FlatDml flatDml);
}
