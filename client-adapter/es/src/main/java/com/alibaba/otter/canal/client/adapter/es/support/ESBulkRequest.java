package com.alibaba.otter.canal.client.adapter.es.support;

import com.alibaba.otter.canal.client.adapter.es.support.model.ESRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;

import java.util.List;

public interface ESBulkRequest {

    List<ESRequest> getRequests();

    void addReqObj(ESRequest request);

    void resetBulk();

    BulkRequest getBulkRequest();

    int numberOfActions();

    ESBulkResponse bulk();

    ESBulkRequest add(DocWriteRequest<?> docWriteRequest);

    interface ESBulkResponse {
        boolean hasFailures();

        void processFailBulkResponse(BulkRequest bulkRequest, String errorMsg);
    }
}
