package com.alibaba.otter.canal.client.adapter.es.support.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.script.Script;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/6/10
 * @Version1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ESRequest {
    private String configFileName;
    private String index;
    private String id;
    private Object source;
    private Script script;
    private String srcOpType;
    private String esOpType;
    private Boolean upsert;
    private Long createTime;
    private String createDate;
}
