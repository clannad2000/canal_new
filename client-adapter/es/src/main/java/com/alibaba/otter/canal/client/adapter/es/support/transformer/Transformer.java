package com.alibaba.otter.canal.client.adapter.es.support.transformer;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.model.TransformContext;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/13
 * @Version1.0
 */
public interface Transformer {
     void transform(ESData esData, TransformContext context);
}
