package com.alibaba.otter.canal.client.adapter.es.support.extractor.processor.impl.tyre;

import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.extractor.processor.AbstractProcessorExtractor;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
import com.alibaba.otter.canal.client.adapter.support.SqlUtil;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/25
 * @Version1.0
 */
@Service
public class TyredbSkuSProductSku extends AbstractProcessorExtractor {
    private List<String> sCompanyColumns = Arrays.asList("area_number",
            "city_number",
            "province_number",
            "audit_status",
            "status",
            "account_status",
            "name",
            "address",
            "service_tel"
    );

    @Override
    protected void preprocess(FlatDml flatDml, ExtractorContext context) {

    }

    @Override
    protected void insert(FlatDml flatDml, ExtractorContext context) {
        Map<String, Object> resMap = SqlUtil.queryForMap(context.getConfig().getDataSourceKey(),
                "select sp.category_id         ,\n" +
                        "       sp.brand_id        ,\n" +
                        "       sp.brand_name      ,\n" +
                        "       sp.del_status      ,\n" +
                        "       sp.customer_no     ,\n" +
                        "       sp.company_type    ,\n" +
                        "       sc.area_number     ,\n" +
                        "       sc.city_number     ,\n" +
                        "       sc.province_number ,\n" +
                        "       sc.audit_status    ,\n" +
                        "       sc.status          ,\n" +
                        "       sc.account_status  ,\n" +
                        "       sc.name            ,\n" +
                        "       sc.address         ,\n" +
                        "       sc.service_tel     \n" +
                        "       from s_product sp\n" +
                        "         left join s_company sc on sp.customer_no = sc.no\n" +
                        "where sp.id = ?;", Collections.singletonList(flatDml.getData().get("spu_id")));

        if (resMap.get("name") == null) {
            Map<String, Object> dmlDate = ESSyncUtil.findDmlDate(context.getDmls(), context.getIndex(), "s_company", "customer_no", resMap.get("customer_no"));
            if (dmlDate != null) {
                for (String column : sCompanyColumns) {
                    resMap.put(column, dmlDate.get(column));
                }
            }
        }

        flatDml.getData().putAll(resMap);
    }

    @Override
    protected void update(FlatDml flatDml, ExtractorContext context) {

    }

    @Override
    protected void delete(FlatDml flatDml, ExtractorContext context) {

    }


}
