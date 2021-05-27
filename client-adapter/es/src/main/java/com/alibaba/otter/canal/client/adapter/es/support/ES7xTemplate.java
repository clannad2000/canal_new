package com.alibaba.otter.canal.client.adapter.es.support;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.emun.ComponentTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.processor.updateByQuery.*;
import lombok.SneakyThrows;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ES7xTemplate implements ESTemplate {

    private static final Logger logger = LoggerFactory.getLogger(ESTemplate.class);

    private static final int MAX_BATCH_SIZE = 1000;

    private ESConnection esConnection;

    private ESBulkRequest esBulkRequest;

    public ESBulkRequest getBulk() {
        return esBulkRequest;
    }

    public void resetBulkRequestBuilder() {
        this.esBulkRequest.resetBulk();
    }

    public ES7xTemplate(ESConnection esConnection) {
        this.esConnection = esConnection;
        this.esBulkRequest = this.esConnection.new ES7xBulkRequest();
    }


    @Override
    public void update(ESSyncConfig.ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {

        //是否查询更新操作
        if (mapping.isUpdateByQuery()) {
            updateByQueryForES(mapping, esFieldData);
            return;
        }


        List<String> ids = getIds(mapping, pkVal);
        update(mapping.get_index(), mapping.isUpsert(), ids, esFieldData);
    }

    /**
     * update by query 暂未实现
     *
     * @param config      配置对象
     * @param paramsTmp   sql查询条件
     * @param esFieldData 数据Map
     */
    @Override
    public void updateByQueryForSql(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData) {
        if (paramsTmp.isEmpty()) {
            return;
        }
    }

    /**
     * update by query for es
     *
     * @param mapping     配置对象
     * @param esFieldData 数据Map
     */
    @SneakyThrows
    @Override
    public void updateByQueryForES(ESSyncConfig.ESMapping mapping, Map<String, Object> esFieldData) {
        UpdateByQueryBuilder updateByQueryBuilder = UpdateByQueryBuilder.getInstance(mapping.getConfigFileName());
        if (updateByQueryBuilder == null)
            throw new RuntimeException("Not found updateByQueryBuilder" + mapping.getConfigFileName());
        UpdateByQueryInfo updateByQueryInfo = updateByQueryBuilder.build(esFieldData, mapping);
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(mapping.get_index());
        Script script = new Script(ScriptType.STORED, null, updateByQueryInfo.getScriptId(), esFieldData);
        updateByQueryRequest.setScript(script);
        updateByQueryRequest.setQuery(updateByQueryInfo.getQuery());

        BulkByScrollResponse scrollResponse = esConnection.getRestHighLevelClient().updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        List<BulkItemResponse.Failure> bulkFailures = scrollResponse.getBulkFailures();
        if (!bulkFailures.isEmpty()) {
            throw new RuntimeException("ES sync commit error: " + bulkFailures);
        }
    }

    /**
     * 通过主键删除数据
     *
     * @param mapping 配置对象
     * @param idVal   主键值
     */
    @Override
    public void delete(ESSyncConfig.ESMapping mapping, Object idVal) {
        List<String> ids = getIds(mapping, idVal);

        //判断是否是主表
        //如果是主表则直接删除文档
        if (mapping.isMain()) {
            delete(mapping.get_index(), ids);
            return;
        }

        //从表则更新除id字段的其他所有字段为null.
        Map<String, Object> esFieldData = new HashMap<>();
        mapping.getProperties().keySet().forEach(key -> esFieldData.put(key, null));
        esFieldData.remove(mapping.get_id());

        //是否查询更新操作
        if (mapping.isUpdateByQuery()) {
            updateByQueryForES(mapping, esFieldData);
            return;
        }

        update(mapping.get_index(), false, ids, esFieldData);
    }


    /**
     * 提交批次
     */
    @Override
    public void commit() {
        if (getBulk().numberOfActions() > 0) {
            ESBulkRequest.ESBulkResponse response = getBulk().bulk();
            resetBulkRequestBuilder();
            if (response.hasFailures()) {
                response.processFailBulkResponse("ES sync commit error ");
            }
        }
    }


    //获取es的文档id.
    @SneakyThrows
    private List<String> getIds(ESSyncConfig.ESMapping mapping, Object idVal) {
        List<String> ids;
        //根据主键模式取得对应的文档id.
        if (mapping.isIdMode()) {
            if (idVal == null) throw new RuntimeException("idVal is not null!");
            //id主键: 直接使用idVal
            ids = Collections.singletonList(idVal.toString());
        } else {
            //pk主键: 查询es获取对应的id.
            SearchRequest searchRequest = new SearchRequest(mapping.get_index())
                    .source(new SearchSourceBuilder().query(QueryBuilders.termQuery(mapping.get_id(), idVal)).size(10000));

            SearchResponse response = esConnection.getRestHighLevelClient().search(searchRequest, RequestOptions.DEFAULT);

            ids = Stream.of(response.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toList());
        }
        return ids;
    }


    //es删除请求
    private void delete(String esIndex, List<String> ids) {
        for (String id : ids) {
            DeleteRequest deleteRequest = new DeleteRequest(esIndex, id);
            getBulk().add(deleteRequest);
            commitBulk();
        }
    }


    //es更新请求
    private void update(String esIndex, Boolean upsert, List<String> ids, Map<String, Object> esFieldData) {
        for (String id : ids) {
            UpdateRequest updateRequest = new UpdateRequest(esIndex, id).doc(esFieldData).docAsUpsert(upsert);
            getBulk().add(updateRequest);
            commitBulk();
        }
    }

    /**
     * 如果大于批量数则提交批次
     */
    public void commitBulk() {
        if (getBulk().numberOfActions() >= MAX_BATCH_SIZE) {
            commit();
        }
    }

}
