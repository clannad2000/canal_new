package com.alibaba.otter.canal.client.adapter.es.support.model;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/13
 * @Version1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransformContext {
    private ESSyncConfig.ESMapping esMapping;
}
