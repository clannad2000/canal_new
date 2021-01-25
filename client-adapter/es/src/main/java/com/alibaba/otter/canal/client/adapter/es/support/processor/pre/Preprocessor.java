package com.alibaba.otter.canal.client.adapter.es.support.processor.pre;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Description 前置数据处理器接口
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public interface Preprocessor {
    Logger logger = LoggerFactory.getLogger(Preprocessor.class);

    void dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping);
}
