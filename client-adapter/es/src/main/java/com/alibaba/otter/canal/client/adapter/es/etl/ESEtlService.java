package com.alibaba.otter.canal.client.adapter.es.etl;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.es.service.ESSyncServiceFactory;
import com.alibaba.otter.canal.client.adapter.es.service.EtlESSyncService;
import com.alibaba.otter.canal.client.adapter.es.support.ES7xTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.ESBulkRequest;
import com.alibaba.otter.canal.client.adapter.es.support.ESConnection;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.load.Loader;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.DataHandler;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.DataHandlerFactory;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
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
    private Loader loader;

    public ESEtlService(ESConnection esConnection, ESSyncConfig config, Loader loader) {
        super("ES", config);
        this.esConnection = esConnection;
        this.esTemplate = new ES7xTemplate(esConnection);
        this.config = config;
        this.loader = loader;
    }

    public EtlResult importData(List<String> params) {
        ESSyncConfig.ESMapping mapping = config.getEsMapping();
        logger.info("start etl to import data to index: {}", mapping.get_index());
        String tableName = mapping.getTableName();
        return importData(tableName, params);
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
                        Map<String, Object> sourceData = new HashMap<>();

                        ResultSetMetaData md = resultSet.getMetaData(); //获得结果集结构信息,元数据
                        int columnCount = md.getColumnCount();   //获得列数
                        for (int i = 1; i <= columnCount; i++) {
                            sourceData.put(md.getColumnLabel(i), resultSet.getObject(i));
                        }

                        DataHandler dataHandler = DataHandlerFactory.getDataHandler(mapping.getConfigFileName());

                        //ESData esData = dataHandler.dispose(config, sourceData, OpTypeEnum.INSERT);

                        //取得主键值
                        //Object idVal = esFieldData.remove(mapping.get_id());

                        //esTemplate.update(mapping, idVal, esFieldData, OpTypeEnum.INSERT);
                        //loader.load(mapping, esData);
                        ESSyncServiceFactory.getInstant(EtlESSyncService.class).sync(ExtractorContext.builder()
                                .config(config)
                                .flatDml(FlatDml.builder().table(config.getEsMapping().getTableName()).type(OpTypeEnum.INSERT).data(sourceData).build())
                                .build());
                        count++;
                        impCount.incrementAndGet();
                    }
                    esTemplate.commit();

                    if (esBulkRequest.numberOfActions() > 0) {
                        long esBatchBegin = System.currentTimeMillis();
                        ESBulkRequest.ESBulkResponse rp = esBulkRequest.bulk();
                        if (rp.hasFailures()) {
                            rp.processFailBulkResponse(esBulkRequest.getBulkRequest(), "全量数据 etl 异常 ");
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
