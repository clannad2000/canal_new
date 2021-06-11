package com.alibaba.otter.canal.client.adapter.es.support.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/22
 * @Version1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpdateByQueryInfo {
    /**
    * es查询语句
    */
    private QueryBuilder query;

    /**
    * 脚本id
    */
    private String scriptId;
}
