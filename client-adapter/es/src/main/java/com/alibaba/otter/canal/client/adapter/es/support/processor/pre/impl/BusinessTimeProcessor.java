package com.alibaba.otter.canal.client.adapter.es.support.processor.pre.impl;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.processor.pre.Preprocessor;

import java.util.List;
import java.util.Map;

/**
 *
 *
 * @Description
 * @Author 黄念
 * @Date 2021/1/15
 * @Version1.0
 */
public class BusinessTimeProcessor implements Preprocessor {

    @Override
    public void dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping) {
        List<String> list = ESSyncUtil.strToList(fieldMapping.getColumn());
        List<String> paramList = ESSyncUtil.strToList(fieldMapping.getParam());

        paramList.forEach(ele->sourceData.put(ele,null));

        Long businessStartTime = getTime(sourceData, list.get(0));
        Long businessEndTime = getTime(sourceData, list.get(1));

        if (businessStartTime == null || businessEndTime == null) return;

        if (businessStartTime.equals(businessEndTime)) {
            sourceData.put(paramList.get(0), 0);
            sourceData.put(paramList.get(1), 24L * 60 * 60 * 1000);
            return;
        }

        if (businessEndTime < businessStartTime) {
            sourceData.put(paramList.get(0), 0);
            sourceData.put(paramList.get(1), businessEndTime);
            sourceData.put(paramList.get(2), businessStartTime);
            sourceData.put(paramList.get(3), 24L * 60 * 60 * 1000);
            return;
        }

        sourceData.put(paramList.get(0), businessStartTime);
        sourceData.put(paramList.get(1), businessEndTime);
    }

    private Long getTime(Map<String, Object> sourceData, String column) {
        List<Integer> list = ESSyncUtil.strToIntList(sourceData.get(column) != null ? sourceData.get(column).toString() : null, ":");
        if (list.isEmpty()) return null;
        return (long) list.get(0) * 60 * 60 * 1000 + list.get(1) * 60 * 1000;
    }
}
