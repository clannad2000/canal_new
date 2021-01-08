package com.alibaba.otter.canal.client.adapter.es.support.handler;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public interface DataMappingHandler {
    Logger logger = LoggerFactory.getLogger(ESSyncUtil.class);

    Object handle(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping);
}
