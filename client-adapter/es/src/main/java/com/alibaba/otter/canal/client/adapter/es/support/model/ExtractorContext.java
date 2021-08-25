package com.alibaba.otter.canal.client.adapter.es.support.model;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/14
 * @Version1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExtractorContext {
    private ESSyncConfig    config;
    private FlatDml         flatDml;
    private List<Dml>       dmls;
    private Integer         index;
//    private OpTypeEnum opTypeEnum;
}
