package com.alibaba.otter.canal.client.adapter.es.support.processor.data;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public interface DataMappingProcessor {
    Logger logger = LoggerFactory.getLogger(DataMappingProcessor.class);

    Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping);
}
