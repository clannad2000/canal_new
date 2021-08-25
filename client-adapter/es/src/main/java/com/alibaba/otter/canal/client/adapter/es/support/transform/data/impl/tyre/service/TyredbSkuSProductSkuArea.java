package com.alibaba.otter.canal.client.adapter.es.support.transform.data.impl.tyre.service;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.AbstractDataHandler;
import com.alibaba.otter.canal.client.adapter.support.SqlUtil;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/6/21
 * @Version1.0
 */
@Service
public class TyredbSkuSProductSkuArea extends AbstractDataHandler {
    @Override
    public ESData postDispose(ESSyncConfig esSyncConfig, Map<String, Object> sourceData, Map<String, Object> esFieldData, OpTypeEnum opTypeEnum) {
        String sql = "select group_concat(area_number) as areaNumberList,group_concat(rent_percent) as rentPercentList\n" +
                "             from s_product_sku_area\n" +
                "             where sku_id = ?\n" +
                "             group by sku_id;";
        Map<String, Object> map = SqlUtil.queryForMap(esSyncConfig.getDataSourceKey(), sql, Collections.singletonList(sourceData.get("sku_id").toString()));

        ESSyncConfig.ESMapping esMapping = esSyncConfig.getEsMapping();

        ESData esData = ESData.builder()
                .srcOpType(opTypeEnum)
                .index(esMapping.get_index())
                //在更新阶段删除主键
                .idVal(esFieldData.remove(esMapping.get_id()).toString())
                .esFieldData(esFieldData)
                .upsert(esSyncConfig.getEsMapping().isUpsert())
                .build();

        if (map.size() == 0) {
            esFieldData.put("areaNumber", new ArrayList<>());
            return esData;
        }

        List<String> areaNumberList = ESSyncUtil.strToList(map.get("areaNumberList"));
        List<String> rentPercentList = ESSyncUtil.strToList(map.get("rentPercentList"));

        Map<String, Object> rentInfo = new HashMap<>();
        esFieldData.put("areaNumber", areaNumberList);
        esFieldData.put("rentInfo", rentInfo);

        for (int i = 0; i < areaNumberList.size(); i++) {
            rentInfo.put(areaNumberList.get(i), Collections.singletonMap("rentPercent", Double.parseDouble(rentPercentList.get(i))));
        }

        if (opTypeEnum == OpTypeEnum.UPDATE) {
            esData.setParams(esFieldData);
            esData.setScript("tyre-sku-s_product_sku_area");
            esData.setSrcOpType(OpTypeEnum.SCRIPTED_UPDATE);
        }

        return esData;
    }
}
