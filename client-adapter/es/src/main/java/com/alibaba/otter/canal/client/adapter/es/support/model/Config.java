package com.alibaba.otter.canal.client.adapter.es.support.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/21
 * @Version1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public  class Config {
    //表名
    private String  table;
    //权重
    private Integer order;
    //组名唯一
    private String  groupName;
}
