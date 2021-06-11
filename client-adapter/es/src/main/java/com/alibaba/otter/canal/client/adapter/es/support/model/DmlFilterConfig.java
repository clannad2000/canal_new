package com.alibaba.otter.canal.client.adapter.es.support.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/6/3
 * @Version1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public  class DmlFilterConfig {

    /**
    * 表名
    */
    private String tableName;

    /**
    * 数据库主键列名
    */
    private String idColumn;
}
