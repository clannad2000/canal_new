//package com.alibaba.otter.canal.client.adapter.es.support.transform.data.impl.tyre.service;
//
//import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
//import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
//import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
//import com.alibaba.otter.canal.client.adapter.es.support.transform.data.AbstractDataHandler;
//import com.alibaba.otter.canal.client.adapter.support.SqlUtil;
//import org.springframework.stereotype.Service;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
///**
// * @Description
// * @Author 黄念
// * @Date 2021/6/22
// * @Version1.0
// */
//@Service
//public class TyredbSkuSProductSkuDetail extends AbstractDataHandler {
//
//    @Override
//    public Map<String, Object> postDispose(ESSyncConfig esSyncConfig, Map<String, Object> sourceData, Map<String, Object> esFieldData, OpTypeEnum opTypeEnum) {
//        String sql = "select spsd.area_number as areaNumber,group_concat(spsd.product_type) as productType," +
//                "group_concat(sps.product_sku_name) as title," +
//                "group_concat(spsd.count) as num," +
//                "group_concat(spsd.original_price) as originalPrice," +
//                "group_concat(spsd.sales_price) as salesPrice" +
//                "    from s_product_sku_detail spsd\n" +
//                "             left join s_product_sku sps on spsd.send_sku_id = sps.id\n" +
//                "    where spsd.sku_id = ?\n" +
//                "    group by spsd.area_number";
//
//        List<Map<String, Object>> mapList = SqlUtil.queryForMapList(esSyncConfig.getDataSourceKey(), sql, Collections.singletonList(sourceData.get("sku_id")));
//        Map<Object, List<Map<String, Object>>> listMap = mapList.stream().filter(map -> map.get("areaNumber") != null).collect(Collectors.toMap(map -> map.get("areaNumber"), this::getSubItems));
//
//        esFieldData.put("subItems", mapList.stream().filter(map -> map.get("areaNumber") == null).map(this::getSubItems).collect(Collectors.toList()));
//        esFieldData.put("rentInfo", listMap);
//        return esFieldData;
//    }
//
//
//    public List<Map<String, Object>> getSubItems(Map<String, Object> map) {
//        List<Integer> productType = ESSyncUtil.strToIntList(map.get("productType"));
//        List<String> title = ESSyncUtil.strToList(map.get("title"));
//        List<String> num = ESSyncUtil.strToList(map.get("num"));
//        List<String> originalPrice = ESSyncUtil.strToList(map.get("originalPrice"));
//        List<String> salesPrice = ESSyncUtil.strToList(map.get("salesPrice"));
//
//        List<Map<String, Object>> list = new ArrayList<>();
//        for (int i = 0; i < title.size(); i++) {
//            Map<String, Object> subItems = new HashMap<>();
//            subItems.put("productType", productType.get(i));
//            subItems.put("title", title.get(i));
//            subItems.put("num", num.get(i));
//            subItems.put("originalPrice", originalPrice.get(i));
//            subItems.put("salesPrice", salesPrice.get(i));
//            list.add(subItems);
//        }
//        return list;
//    }
//}
//
