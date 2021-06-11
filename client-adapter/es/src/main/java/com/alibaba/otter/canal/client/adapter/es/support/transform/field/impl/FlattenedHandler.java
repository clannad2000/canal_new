package com.alibaba.otter.canal.client.adapter.es.support.transform.field.impl;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.emun.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.es.support.transform.field.FieldMappingHandler;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/5/11
 * @Version1.0 {"key": "k",value: "v"} --> {"k":v"}
 * <p>
 * 数字类型: 整数13位,小数2位
 * 整数:
 * 整数位不足前面补零.
 * 小数:
 * 小数位不足后面补零.
 */
public class FlattenedHandler implements FieldMappingHandler {

    @Override
    public Object dispose(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, OpTypeEnum opTypeEnum) {
        List<String> dataField = ESSyncUtil.strToList(fieldMapping.getColumn());
        Object key = sourceData.get(dataField.get(0));
        Object value = sourceData.get(dataField.get(1));
        if (key == null) return null;
//
//        Object dataType = sourceData.get(dataField.get(2));
//        if (dataType != null && value != null) {
//            String strVal = value.toString();
//            switch (DataTypeEnum.match(dataType.toString())) {
//                case SHORT:
//                case INT:
//                case LONG:
//                    strVal = "0000000000000" + strVal;
//                    value = strVal.substring(strVal.length() - 13);
//                    break;
//                case FLOAT:
//                case DOUBLE:
//                    String[] split = strVal.split("\\.");
//                    String s1 = "0000000000000" + split[0];
//                    s1 = s1.substring(s1.length() - 13);
//                    String s2 = split[1] + "00";
//                    s2 = s2.substring(0, 2);
//                    value = s1 + '.' + s2;
//                    break;
//            }
//        }

        return Collections.singletonMap(key, value);
    }
}
