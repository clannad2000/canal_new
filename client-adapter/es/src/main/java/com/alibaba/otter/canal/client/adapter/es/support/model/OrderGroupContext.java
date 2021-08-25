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
public class OrderGroupContext implements Comparable<Integer> {
    //事务提交时间
    private Long    es;
    //组名
    private String  groupName;
    //组数据大小
    private Integer groupSize;
    //表名
    private String  tableName;
    //权重
    private Integer order;
    //索引
    private Integer index;

    @Override
    public int compareTo(Integer o) {
        return this.index.compareTo(o);
    }
}
