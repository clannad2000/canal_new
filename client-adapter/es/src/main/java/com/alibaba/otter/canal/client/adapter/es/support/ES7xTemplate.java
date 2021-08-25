package com.alibaba.otter.canal.client.adapter.es.support;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.emun.ParamsSrcType;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import lombok.SneakyThrows;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.LocalDateTime;
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
    public void update(ESData esData) {
        try {
            UpdateRequest updateRequest = new UpdateRequest(esData.getIndex(), esData.getIdVal()).doc(esData.getEsFieldData()).docAsUpsert(esData.isUpsert());
            getBulk().add(updateRequest);
            //getBulk().addReqObj(ESRequest.builder().index(esIndex).id(id).source(esFieldData).upsert(upsert).esOpType(DocWriteRequest.OpType.UPDATE.name()).configFileName(configFileName).srcOpType(opTypeEnum.name()).createTime(System.currentTimeMillis()).createDate(LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss")).build());
            commitBulk();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        //List<String> ids = getIds(mapping, pkVal);
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
    public void updateByQuery(ESData esData) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(esData.getIndex());
        Script script = new Script(ScriptType.STORED, null, esData.getScript(), esData.getParamsSrc() == ParamsSrcType.PARAMS ? esData.getParams() : esData.getEsFieldData());

        updateByQueryRequest.setScript(script);
        updateByQueryRequest.setQuery(esData.getQuery());

        BulkByScrollResponse scrollResponse = esConnection.getRestHighLevelClient().updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        saveErrorLog(scrollResponse, updateByQueryRequest);
    }


    private void saveErrorLog(BulkByScrollResponse scrollResponse, UpdateByQueryRequest updateByQueryRequest) {
        List<BulkItemResponse.Failure> bulkFailures = scrollResponse.getBulkFailures();
        if (!bulkFailures.isEmpty()) {
            BulkRequest errorLogRequest = new BulkRequest();
            for (BulkItemResponse.Failure bulkFailure : bulkFailures) {
                Map<String, Object> map = new HashMap<>();
                map.put("statusName", bulkFailure.getStatus().name());
                map.put("status", bulkFailure.getStatus().getStatus());
                map.put("failureMessage", bulkFailure.getCause());
                map.put("request", updateByQueryRequest);
                map.put("createTime", System.currentTimeMillis());
                map.put("createDate", LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                IndexRequest indexRequest = new IndexRequest();
                indexRequest.index("canal-adapter_es_error_" + LocalDateTime.now().toString("yyyy-MM-dd"));
                indexRequest.source(GsonUtil.gson.toJson(map), XContentType.JSON);
                errorLogRequest.add(indexRequest);
            }

            esConnection.getRestHighLevelClient().bulkAsync(errorLogRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            });

            throw new RuntimeException("ES update by query sync commit error: " + bulkFailures);
        }
    }


    /**
     * 通过主键删除数据
     *
     * @param mapping 配置对象
     * @param idVal   主键值
     */
    @Override
    public void delete(ESData esData) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest(esData.getIndex(), esData.getIdVal());
            getBulk().add(deleteRequest);
            //getBulk().addReqObj(ESRequest.builder().index(esIndex).id(id).esOpType(DocWriteRequest.OpType.DELETE.name()).configFileName(configFileName).srcOpType(opTypeEnum.name()).createTime(System.currentTimeMillis()).createDate(LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss")).build());
            commitBulk();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }


    /**
     * 提交批次
     */
    @SneakyThrows
    @Override
    public void commit() {
        if (getBulk().numberOfActions() > 0) {
            ESBulkRequest esBulkRequest = getBulk();
            BulkRequest bulkRequest = esBulkRequest.getBulkRequest();
            ESBulkRequest.ESBulkResponse response = esBulkRequest.bulk();

//            Map<String, Object> map = new HashMap<>();
//            map.put("createTime", System.currentTimeMillis());
//            map.put("createDate", LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
//            map.put("requests", esBulkRequest.getRequests());
//            String json = GsonUtil.gson.toJson(map);
//            IndexRequest indexRequest = new IndexRequest().index("canal-adapter_es_requests_"+LocalDateTime.now().toString("yyyy-MM-dd")).source(json, XContentType.JSON);
//            esConnection.getRestHighLevelClient().index(indexRequest, RequestOptions.DEFAULT);

            try {
                if (response.hasFailures()) {
                    response.processFailBulkResponse(bulkRequest, "ES sync commit error ");
                }
            } catch (Exception e) {
                throw e;
            } finally {
                resetBulkRequestBuilder();
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


//    //删除
//    private void delete(String esIndex, String id, OpTypeEnum opTypeEnum) {
//        try {
//            DeleteRequest deleteRequest = new DeleteRequest(esIndex, id);
//            getBulk().add(deleteRequest);
//            //getBulk().addReqObj(ESRequest.builder().index(esIndex).id(id).esOpType(DocWriteRequest.OpType.DELETE.name()).configFileName(configFileName).srcOpType(opTypeEnum.name()).createTime(System.currentTimeMillis()).createDate(LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss")).build());
//            commitBulk();
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw e;
//        }
//    }


    //脚本更新
    @Override
    public void scriptUpdate(ESData esData) {
        try {
            Script script = new Script(ScriptType.STORED, null, esData.getScript(), esData.getParamsSrc() == ParamsSrcType.PARAMS ? esData.getParams() : esData.getEsFieldData());
            UpdateRequest updateRequest = new UpdateRequest(esData.getIndex(), esData.getIdVal()).script(script);
            getBulk().add(updateRequest);
            //getBulk().addReqObj(ESRequest.builder().index(mapping.get_index()).id(pkVal).script(script).esOpType(DocWriteRequest.OpType.UPDATE.name()).configFileName(mapping.getConfigFileName()).srcOpType(opTypeEnum.name()).createTime(System.currentTimeMillis()).createDate(LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss")).build());
            commitBulk();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
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
