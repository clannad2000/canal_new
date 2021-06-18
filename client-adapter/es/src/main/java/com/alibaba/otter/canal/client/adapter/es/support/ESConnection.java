package com.alibaba.otter.canal.client.adapter.es.support;

import com.alibaba.otter.canal.client.adapter.es.support.model.ESRequest;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentType;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ES 连接器, Rest 一种方式
 *
 * @author rewerma 2019-08-01
 * @version 1.0.0
 */
@Data
public class ESConnection {

    private static final Logger logger = LoggerFactory.getLogger(ESConnection.class);

    private static RestHighLevelClient restHighLevelClient;


    public ESConnection(String[] hosts, Map<String, String> properties) throws UnknownHostException {
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            String host = hosts[i];
            int j = host.indexOf(":");
            HttpHost httpHost = new HttpHost(InetAddress.getByName(host.substring(0, j)),
                    Integer.parseInt(host.substring(j + 1)));
            httpHosts[i] = httpHost;
        }
        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
        String nameAndPwd = properties.get("security.auth");
        if (StringUtils.isNotEmpty(nameAndPwd) && nameAndPwd.contains(":")) {
            String[] nameAndPwdArr = nameAndPwd.split(":");
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(nameAndPwdArr[0],
                    nameAndPwdArr[1]));
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        restHighLevelClient = new RestHighLevelClient(restClientBuilder);

    }

    public void close() {
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public MappingMetaData getMapping(String index) {
        Map<String, MappingMetaData> mappings;
        AtomicReference<String> writeIndex = new AtomicReference<>();
        try {
            GetAliasesRequest aliasesRequest = new GetAliasesRequest();
            aliasesRequest.indices(index);
            GetAliasesResponse aliasesResponse = restHighLevelClient.indices().getAlias(aliasesRequest, RequestOptions.DEFAULT);
            aliasesResponse.getAliases().forEach((key, aliasMetaDataSet) -> aliasMetaDataSet.forEach(aliasMetaData -> {
                if (aliasMetaData.getAlias().equals(index) && aliasMetaData.writeIndex()) {
                    writeIndex.set(key);
                }
            }));

            GetMappingsRequest mappingsRequest = new GetMappingsRequest();
            mappingsRequest.indices(writeIndex.get());
            GetMappingsResponse response = restHighLevelClient.indices()
                    .getMapping(mappingsRequest, RequestOptions.DEFAULT);
            mappings = response.mappings();
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Not found the mapping info of index: " + index);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
        return mappings.get(writeIndex.get());
    }

    public RestHighLevelClient getRestHighLevelClient() {
        return restHighLevelClient;
    }


    @Data
    public class ES7xBulkRequest implements ESBulkRequest {

        private BulkRequestBuilder bulkRequestBuilder;

        private BulkRequest bulkRequest;

        private List<ESRequest> requests;

        @Override
        public void addReqObj(ESRequest request) {
            requests.add(request);
        }

        public ES7xBulkRequest() {
            bulkRequest = new BulkRequest();
            requests = new ArrayList<>();
        }

        public void resetBulk() {
            bulkRequest = new BulkRequest();
            requests = new ArrayList<>();
        }

        public int numberOfActions() {
            return bulkRequest.numberOfActions();
        }

        public ESBulkResponse bulk() {
            try {
                BulkResponse responses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                return new ES7xBulkResponse(requests, responses);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ESBulkRequest add(DocWriteRequest<?> docWriteRequest) {
            bulkRequest.add(docWriteRequest);
            return this;
        }
    }

    public static class ES7xBulkResponse implements ESBulkRequest.ESBulkResponse {

        private List<ESRequest> requests;

        private BulkResponse bulkResponse;

        public ES7xBulkResponse(List<ESRequest> requests, BulkResponse bulkResponse) {
            this.requests = requests;
            this.bulkResponse = bulkResponse;
        }

        @Override
        public boolean hasFailures() {
            return bulkResponse.hasFailures();
        }

        @SneakyThrows
        @Override
        public void processFailBulkResponse(BulkRequest bulkRequest, String errorMsg) {
            BulkItemResponse[] items = bulkResponse.getItems();
            BulkRequest errorLogRequest = new BulkRequest();
            for (int i = 0; i < items.length; i++) {
                BulkItemResponse itemResponse = items[i];
                if (!itemResponse.isFailed()) {
                    continue;
                }

                logger.error(errorMsg + itemResponse.getFailureMessage());
                Map<String, Object> map = new HashMap<>();
                map.put("statusName", itemResponse.status().name());
                map.put("status", itemResponse.status().getStatus());
                map.put("failureMessage", itemResponse.getFailureMessage());
                map.put("request", requests.get(i));
                map.put("createTime", System.currentTimeMillis());
                map.put("createDate", LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                IndexRequest indexRequest = new IndexRequest();
                indexRequest.index("canal-adapter_es_error_"+LocalDateTime.now().toString("yyyy-MM-dd"));
                indexRequest.source(GsonUtil.gson.toJson(map), XContentType.JSON);
                errorLogRequest.add(indexRequest);
            }
            restHighLevelClient.bulkAsync(errorLogRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                }
                @Override
                public void onFailure(Exception e) {
                }
            });

        }
    }
}
