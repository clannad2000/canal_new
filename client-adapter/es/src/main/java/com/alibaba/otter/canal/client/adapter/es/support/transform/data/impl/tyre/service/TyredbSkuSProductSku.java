package com.alibaba.otter.canal.client.adapter.es.support.transform.data.impl.tyre.service;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.AbstractDataHandler;
import com.alibaba.otter.canal.client.adapter.support.SqlUtil;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/25
 * @Version1.0
 */
@Service
public class TyredbSkuSProductSku extends AbstractDataHandler {

    /** 后置处理
     * @param esSyncConfig 配置
     * @param sourceData   源数据
     * @param esFieldData  es数据
     * @param opTypeEnum   操作类型
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author 黄念
     * @date 2021/6/11 11:03
     */
    @Override
    public Map<String, Object> postDispose(ESSyncConfig esSyncConfig, Map<String, Object> sourceData, Map<String, Object> esFieldData, OpTypeEnum opTypeEnum) {
        //添加spu信息,所属机构信息
        if (opTypeEnum == OpTypeEnum.INSERT) {
            Map<String, Object> map = SqlUtil.queryForMap(esSyncConfig.getDataSourceKey(),
                    "select sp.category_id         as categoryId,\n" +
                            "       sp.brand_id        as brandId,\n" +
                            "       sp.brand_name      as brandName,\n" +
                            "       sp.del_status      as spuDelStatus,\n" +
                            "       sp.customer_no     as customerNo,\n" +
                            "       sp.company_type    as companyType,\n" +
                            "       sc.area_number     as companyAreaNumber,\n" +
                            "       sc.city_number     as companyCityNumber,\n" +
                            "       sc.province_number as companyProvinceNumber,\n" +
                            "       sc.audit_status    as companyAuditStatus,\n" +
                            "       sc.status          as companyStatus,\n" +
                            "       sc.account_status  as accountStatus,\n" +
                            "       sc.name            as name,\n" +
                            "       sc.address         as address,\n" +
                            "       sc.service_tel     as serviceTel\n" +
                            "       from s_product sp\n" +
                            "         left join s_company sc on sp.customer_no = sc.no\n" +
                            "where sp.id = ?;", Collections.singletonList(esFieldData.get("spuId")));
            esFieldData.putAll(map);
        }
        return esFieldData;
    }
}

