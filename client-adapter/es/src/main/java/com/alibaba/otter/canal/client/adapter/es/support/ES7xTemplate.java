package com.alibaba.otter.canal.client.adapter.es.support;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import lombok.SneakyThrows;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
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


    /**
     * 插入数据
     *
     * @param mapping     配置对象
     * @param pkVal       主键值
     * @param esFieldData 数据Map
     */
    @Override
    public void insert(ESSyncConfig.ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        update(mapping, pkVal, esFieldData);
    }


    @Override
    public void update(ESSyncConfig.ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        List<String> ids = getIds(mapping, pkVal);

        if (mapping.isMain()) {
            update(mapping.get_index(), true, ids, esFieldData);
            return;
        }
        update(mapping.get_index(), false, ids, esFieldData);
    }

    /**
     * update by query 暂未实现
     *
     * @param config      配置对象
     * @param paramsTmp   sql查询条件
     * @param esFieldData 数据Map
     */
    @Override
    public void updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData) {
        if (paramsTmp.isEmpty()) {
            return;
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
    @SneakyThrows
    private void delete(String esIndex, List<String> ids) {
        for (String id : ids) {
            DeleteRequest deleteRequest = new DeleteRequest(esIndex, id);
            getBulk().add(deleteRequest);
            commitBulk();
        }
    }


    //es更新请求
    @SneakyThrows
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
