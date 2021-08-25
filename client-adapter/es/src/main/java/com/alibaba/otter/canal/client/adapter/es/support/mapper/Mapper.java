package com.alibaba.otter.canal.client.adapter.es.support.mapper;

import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.model.MapperContext;

import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/15
 * @Version1.0
 */
public interface Mapper {
   void mapping(ESData esData, MapperContext context);
}
