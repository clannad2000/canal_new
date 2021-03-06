package com.alibaba.otter.canal.client.adapter.es.support.transform.field;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OpTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

/** 通用数据处理器
 * @Description
 * @Author 黄念
 * @Date 2020/12/21
 * @Version1.0
 */
public interface FieldMappingHandler {
    Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, OpTypeEnum opTypeEnum);
}
