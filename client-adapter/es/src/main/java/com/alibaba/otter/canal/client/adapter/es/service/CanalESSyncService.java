package com.alibaba.otter.canal.client.adapter.es.service;

import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.EnumUtils;
import com.alibaba.otter.canal.client.adapter.es.support.load.Loader;
import com.alibaba.otter.canal.client.adapter.es.support.model.ExtractorContext;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.FlatDml;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/22
 * @Version1.0
 */
public class CanalESSyncService extends AbstractESSyncService {

    public CanalESSyncService(ESTemplate esTemplate, Loader loader) {
        super(esTemplate, loader);
    }

    @Override
    public void sync(ExtractorContext extractorContext) {
        //查询->过滤->提取->转换->映射->载入
        //dml顺序修改后是否会对其他的任务产生影响
        //解决方案2: 回查, 写专用的回查接口
        List<FlatDml> flatDmlList = buildFlatDmlList(extractorContext.getDmls().get(extractorContext.getIndex()));
        for (FlatDml flatDml : flatDmlList) {
            extractorContext.setFlatDml(flatDml);
            sync2(extractorContext);
        }
    }

    public List<FlatDml> buildFlatDmlList(Dml dml) {
        List<FlatDml> list = new ArrayList<>(dml.getData().size());
        for (int i = 0; i < dml.getData().size(); i++) {
            FlatDml flatDml = FlatDml.builder()
                    .destination(dml.getDestination())
                    .groupId(dml.getGroupId())
                    .database(dml.getDatabase())
                    .table(dml.getTable())
                    .isDdl(dml.getIsDdl())
                    .es(dml.getEs())
                    .ts(dml.getTs())
                    .pkNames(dml.getPkNames())
                    .sql(dml.getSql())
                    .type(EnumUtils.getInstance(OpTypeEnum.class,dml.getType().toLowerCase()))
                    .data(dml.getData().get(i))
                    .old(dml.getOld() != null ? dml.getOld().get(i) : null)
                    .build();
            list.add(flatDml);
        }
        return list;
    }

}
