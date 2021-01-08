package com.alibaba.otter.canal.client.adapter.es.support;

import org.elasticsearch.action.DocWriteRequest;

public interface ESBulkRequest {

    void resetBulk();

    int numberOfActions();

    ESBulkResponse bulk();

    ESBulkRequest add(DocWriteRequest<?> docWriteRequest);

    interface ESBulkResponse {
        boolean hasFailures();

        void processFailBulkResponse(String errorMsg);
    }
}
