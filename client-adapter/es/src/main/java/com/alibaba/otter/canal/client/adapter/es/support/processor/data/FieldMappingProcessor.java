package com.alibaba.otter.canal.client.adapter.es.support.processor.data;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OperationEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public interface FieldMappingProcessor {
    Logger logger = LoggerFactory.getLogger(FieldMappingProcessor.class);

    JdbcTemplate jdbcTemplate = new JdbcTemplate();

    Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, OperationEnum operationEnum);
}
