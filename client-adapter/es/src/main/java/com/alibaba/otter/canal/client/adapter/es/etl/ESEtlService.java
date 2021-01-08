package com.alibaba.otter.canal.client.adapter.es.etl;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.es.support.ES7xTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.ESBulkRequest;
import com.alibaba.otter.canal.client.adapter.es.support.ESConnection;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * ES ETL Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESEtlService extends AbstractEtlService {

    private ESConnection esConnection;
    private ESTemplate esTemplate;
    private ESSyncConfig config;

    public ESEtlService(ESConnection esConnection, ESSyncConfig config) {
        super("ES", config);
        this.esConnection = esConnection;
        this.esTemplate = new ES7xTemplate(esConnection);
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        ESSyncConfig.ESMapping mapping = config.getEsMapping();
        logger.info("start etl to import data to index: {}", mapping.get_index());
        String sql = mapping.getSql();
        return importData(sql, params);
    }

    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
                                       AdapterConfig.AdapterMapping adapterMapping, AtomicLong impCount,
                                       List<String> errMsg) {
        try {
            ESSyncConfig.ESMapping mapping = (ESSyncConfig.ESMapping) adapterMapping;
            Util.sqlRS(ds, sql, values, resultSet -> {
                int count = 0;
                try {
                    ESBulkRequest esBulkRequest = this.esConnection.new ES7xBulkRequest();
                    long batchBegin = System.currentTimeMillis();
                    while (resultSet.next()) {
                        Map<String, Object> esFieldData = new HashMap<>();
                        Map<String, Object> sourceData = new HashMap<>();

                        ResultSetMetaData md = resultSet.getMetaData(); //获得结果集结构信息,元数据
                        int columnCount = md.getColumnCount();   //获得列数
                        for (int i = 1; i <= columnCount; i++) {
                            sourceData.put(md.getColumnLabel(i),resultSet.getObject(i));
                        }

                        mapping.getProperties().forEach((esFieldName, fieldMapping) -> {
                            Object value = ESSyncUtil.dataMapping(sourceData, fieldMapping, esFieldName);
                            esFieldData.put(esFieldName, value);
                        });

                        //取得主键值
                        Object idVal = esFieldData.remove(mapping.get_id());

                        esTemplate.insert(mapping, idVal, esFieldData);
                        count++;
                        impCount.incrementAndGet();
                    }
                    esTemplate.commit();

                    if (esBulkRequest.numberOfActions() > 0) {
                        long esBatchBegin = System.currentTimeMillis();
                        ESBulkRequest.ESBulkResponse rp = esBulkRequest.bulk();
                        if (rp.hasFailures()) {
                            rp.processFailBulkResponse("全量数据 etl 异常 ");
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("全量数据批量导入最后批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                    (System.currentTimeMillis() - batchBegin),
                                    (System.currentTimeMillis() - esBatchBegin),
                                    esBulkRequest.numberOfActions(),
                                    mapping.get_index());
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    errMsg.add(mapping.get_index() + " etl failed! ==>" + e.getMessage());
                    throw new RuntimeException(e);
                }
                return count;
            });

            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }
}
