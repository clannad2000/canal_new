package com.alibaba.otter.canal.client.adapter.es.support.extractor.processor.impl.tyre;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.extractor.processor.AbstractProcessorExtractor;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.support.SqlUtil;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/15
 * @Version1.0
 */
@Service
public class TyredbSkuSProductSkuArea extends AbstractProcessorExtractor {


    @Override
    protected void preprocess(FlatDml flatDml, ExtractorContext context) {
        String sql = "select spsa.area_number                    as areaNumber,\n" +
                "       spsa.rent_percent                   as rentPercent,\n" +
                "       spsa.rent_percent * sps.sales_price/100 as rentPrice\n" +
                "from s_product_sku_area spsa\n" +
                "         left join s_product_sku sps\n" +
                "                   on spsa.sku_id = sps.id\n" +
                "where sku_id = ?";
        List<Map<String, Object>> mapList = SqlUtil.queryForMapList(context.getConfig().getDataSourceKey(), sql, Collections.singletonList(flatDml.getData().get("sku_id").toString()));

        Map<String, Object> map = new HashMap<>();
        map.put("sku_id", flatDml.getData().get("sku_id"));
        map.put("areaInfo", mapList.size() != 0 ? mapList : new HashMap<>());
        flatDml.setData(map);
    }

    @Override
    protected void insert(FlatDml flatDml, ExtractorContext context) {

    }

    @Override
    protected void update(FlatDml flatDml, ExtractorContext context) {

    }

    @Override
    protected void delete(FlatDml flatDml, ExtractorContext context) {

    }

}
