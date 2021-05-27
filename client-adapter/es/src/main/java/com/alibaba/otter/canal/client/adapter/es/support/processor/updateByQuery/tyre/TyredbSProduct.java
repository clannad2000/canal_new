package com.alibaba.otter.canal.client.adapter.es.support.processor.updateByQuery.tyre;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.processor.updateByQuery.UpdateByQueryInfo;
import com.alibaba.otter.canal.client.adapter.es.support.processor.updateByQuery.UpdateByQueryBuilder;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @Description 通过查询spu表填充数据到sku表对应的索引中
 * @Author 黄念
 * @Date 2021/5/22
 * @Version1.0
 */
@Service
public class TyredbSProduct extends UpdateByQueryBuilder {


    @Override
    public UpdateByQueryInfo build(Map<String, Object> esFieldData, ESSyncConfig.ESMapping mapping) {
        //更新对应的sku
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get("tyredb");
        jdbcTemplate.setDataSource(dataSource);
        List<String> skuIds = jdbcTemplate.queryForList("select id from s_product_sku where spu_id = ?", String.class, esFieldData.get("_id"));
        return UpdateByQueryInfo.builder()
                .query(QueryBuilders.termsQuery("skuId", skuIds))
                .scriptId("tyre-s_product_sku-set_spu_info")
                .build();
    }
}
