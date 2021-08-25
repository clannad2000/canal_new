package com.alibaba.otter.canal.client.adapter.es.support.model;

import com.alibaba.otter.canal.client.adapter.es.support.emun.ParamsSrcType;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/6
 * @Version1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ESData {

    //索引
    private String index;

    private String idVal;

    //源操作类型
    private OpTypeEnum srcOpType;

    //目标操作类型
    private OpTypeEnum dstOpType;

    //插入更新
    private boolean upsert = false;

    //脚本id
    private String script;

    //参数来源
    private ParamsSrcType paramsSrc = ParamsSrcType.DOC;

    //脚本参数
    private Map<String, Object> params;

    //查询更新->查询条件
    private QueryBuilder query;

    private FlatDml flatDml;

    private Map<String, Object> sourceMap;

    //数据
    private Map<String, Object> esFieldData;


    public OpTypeEnum getDstOpType() {
        return dstOpType != null ? dstOpType : srcOpType;
    }
}
