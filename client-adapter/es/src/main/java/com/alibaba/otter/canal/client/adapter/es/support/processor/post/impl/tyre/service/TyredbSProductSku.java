package com.alibaba.otter.canal.client.adapter.es.support.processor.post.impl.tyre.service;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OperationEnum;
import com.alibaba.otter.canal.client.adapter.es.support.processor.post.Postprocessor;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/25
 * @Version1.0
 */
@Service
public class TyredbSProductSku extends Postprocessor {

    @Override
    public void dispose(ESSyncConfig syncConfig, Map<String, Object> sourceData, Map<String, Object> esFieldData, OperationEnum operationEnum) {
        //是否有货字段
        Object cacheNum = sourceData.get("cache_num");
        Object sellNum = sourceData.get("sell_num");
        if (cacheNum == null || sellNum == null) return;
        esFieldData.put("stockStatus", Integer.parseInt(cacheNum.toString()) - Integer.parseInt(sellNum.toString()) > 0);
    }
}
