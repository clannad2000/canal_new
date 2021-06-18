package com.alibaba.otter.canal.client.adapter.es.support;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESRequest;
import com.alibaba.otter.canal.client.adapter.es.support.model.UpdateByQueryInfo;
import lombok.SneakyThrows;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
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

    // private List<Object> requests = new ArrayList<>();

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
    public void update(ESSyncConfig.ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, OpTypeEnum opTypeEnum) {

        //是否查询更新操作
        if (mapping.getUpdateByQuery() != null) {
            updateByQueryForES(mapping, esFieldData);
            return;
        }

        //List<String> ids = getIds(mapping, pkVal);
        update(mapping.get_index(), mapping.isUpsert(), pkVal.toString(), esFieldData, mapping.getConfigFileName(), opTypeEnum);
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

        UpdateByQueryInfo updateByQueryInfo = UpdateByQueryInfo.builder()
                .query(QueryBuilders.termsQuery(mapping.getUpdateByQuery().getPk(), esFieldData.get(mapping.getUpdateByQuery().getPk())))
                .scriptId(mapping.getUpdateByQuery().getScriptId())
                .build();

        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(mapping.get_index());
        Script script = new Script(ScriptType.STORED, null, updateByQueryInfo.getScriptId(), esFieldData);
        updateByQueryRequest.setScript(script);
        updateByQueryRequest.setQuery(updateByQueryInfo.getQuery());

        BulkByScrollResponse scrollResponse = esConnection.getRestHighLevelClient().updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);


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
    public void delete(ESSyncConfig.ESMapping mapping, String idVal, Map<String, Object> esFieldData, OpTypeEnum opTypeEnum) {
        //List<String> ids = getIds(mapping, idVal);

        //判断是否是主表
        //如果是主表则直接删除文档
        if (mapping.isMain()) {
            delete(mapping.get_index(), idVal, mapping.getConfigFileName(), opTypeEnum);
            return;
        }

        //是否查询更新操作
        //因为有查询更新,原pk模式更新已经不需要
        if (mapping.getUpdateByQuery() != null) {
            updateByQueryForES(mapping, Collections.singletonMap(mapping.get_id(), idVal));
            return;
        }

        //从表则更新除id字段的其他所有字段为null.
        mapping.getProperties().keySet().forEach(key -> esFieldData.put(key, null));
        esFieldData.remove(mapping.get_id());
        update(mapping.get_index(), false, idVal, esFieldData, mapping.getConfigFileName(), opTypeEnum);
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


    //删除
    private void delete(String esIndex, String id, String configFileName, OpTypeEnum opTypeEnum) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest(esIndex, id);
            getBulk().add(deleteRequest);
            getBulk().addReqObj(ESRequest.builder().index(esIndex).id(id).esOpType(DocWriteRequest.OpType.DELETE.name()).configFileName(configFileName).srcOpType(opTypeEnum.name()).createTime(System.currentTimeMillis()).createDate(LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss")).build());
            commitBulk();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    //更新
    private void update(String esIndex, boolean upsert, String id, Map<String, Object> esFieldData, String configFileName, OpTypeEnum opTypeEnum) {
        try {
            UpdateRequest updateRequest = new UpdateRequest(esIndex, id).doc(esFieldData).docAsUpsert(upsert);
            getBulk().add(updateRequest);
            getBulk().addReqObj(ESRequest.builder().index(esIndex).id(id).source(esFieldData).upsert(upsert).esOpType(DocWriteRequest.OpType.UPDATE.name()).configFileName(configFileName).srcOpType(opTypeEnum.name()).createTime(System.currentTimeMillis()).createDate(LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss")).build());
            commitBulk();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    //脚本更新
    @Override
    public void scriptUpdate(ESSyncConfig.ESMapping mapping, String pkVal, Script script, OpTypeEnum opTypeEnum) {
        try {
            UpdateRequest updateRequest = new UpdateRequest(mapping.get_index(), pkVal).script(script);
            getBulk().add(updateRequest);
            getBulk().addReqObj(ESRequest.builder().index(mapping.get_index()).id(pkVal).script(script).esOpType(DocWriteRequest.OpType.UPDATE.name()).configFileName(mapping.getConfigFileName()).srcOpType(opTypeEnum.name()).createTime(System.currentTimeMillis()).createDate(LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss")).build());
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
