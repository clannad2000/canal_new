package com.alibaba.otter.canal.client.adapter.es.support.processor.post.impl.tyre.service;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OperationEnum;
import com.alibaba.otter.canal.client.adapter.es.support.processor.post.Postprocessor;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/25
 * @Version1.0
 */
@Service
public class TyredbSGarageInfo extends Postprocessor {

    @Override
    public void dispose(ESSyncConfig esSyncConfig, Map<String, Object> sourceData, Map<String, Object> esFieldData, OperationEnum operationEnum) {
        List<String> paramList = Arrays.asList("businessStartTimeQ1", "businessEndTimeQ1", "businessStartTimeQ2", "businessEndTimeQ2");

        Long businessStartTime = getTime(sourceData, "business_start_time");
        Long businessEndTime = getTime(sourceData, "business_end_time");

        if (businessStartTime == null || businessEndTime == null) return;

        if (businessStartTime.equals(businessEndTime)) {
            esFieldData.put(paramList.get(0), 0);
            esFieldData.put(paramList.get(1), 24L * 60 * 60 * 1000);
            return;
        }

        if (businessEndTime < businessStartTime) {
            esFieldData.put(paramList.get(0), 0);
            esFieldData.put(paramList.get(1), businessEndTime);
            esFieldData.put(paramList.get(2), businessStartTime);
            esFieldData.put(paramList.get(3), 24L * 60 * 60 * 1000);
            return;
        }

        esFieldData.put(paramList.get(0), businessStartTime);
        esFieldData.put(paramList.get(1), businessEndTime);
    }

    private Long getTime(Map<String, Object> sourceData, String column) {
        List<Integer> list = ESSyncUtil.strToIntList(sourceData.get(column) != null ? sourceData.get(column).toString() : null, ":");
        if (list.isEmpty()) return null;
        return (long) list.get(0) * 60 * 60 * 1000 + list.get(1) * 60 * 1000;
    }
}

