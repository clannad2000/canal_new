package com.alibaba.otter.canal.client.adapter.es.support.load;


import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;


/**
 * @Description
 * @Author 黄念
 * @Dat 2021/7/5
 * @Version1.0
 */
public interface Loader {
    void load(ESData esData, ESSyncConfig.ESMapping mapping);
}
