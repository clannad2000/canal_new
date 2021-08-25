package com.alibaba.otter.canal.client.adapter.es.support.load;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.emun.ParamsSrcType;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/6
 * @Version1.0
 */
public class ESLoader extends AbstractLoader {
    ESTemplate esTemplate;

    public ESLoader(ESTemplate esTemplate) {
        this.esTemplate = esTemplate;
    }

    @Override
    public void load(ESData esData, ESSyncConfig.ESMapping mapping) {

        switch (esData.getDstOpType()) {
            case INSERT:
            case UPDATE:
                esTemplate.update(esData);
                break;
            case SCRIPTED_UPDATE:
                if (mapping.getScriptedUpdate() != null && esData.getParamsSrc() == ParamsSrcType.DOC) {
                    esData.setScript(mapping.getUpdateByQuery().getScriptId());
                    esData.getEsFieldData().remove(mapping.get_id());
                    if (esData.getSrcOpType() == OpTypeEnum.DELETE) {
                        esData.getEsFieldData().replaceAll((k, v) -> null);
                    }
                }
                esTemplate.scriptUpdate(esData);
                break;
            case UPDATE_BY_QUERY:
                if (mapping.getUpdateByQuery() != null && esData.getParamsSrc() == ParamsSrcType.DOC) {
                    esData.setQuery(QueryBuilders.termsQuery(mapping.get_id(), esData.getEsFieldData().get(mapping.get_id())));
                    esData.setScript(mapping.getUpdateByQuery().getScriptId());
                    esData.getEsFieldData().remove(mapping.get_id());
                    if (esData.getSrcOpType() == OpTypeEnum.DELETE) {
                        esData.getEsFieldData().replaceAll((k, v) -> null);
                    }
                }
                esTemplate.updateByQuery(esData);
                break;
            case DELETE:
                //主表
                if (mapping.isMain()) {
                    esTemplate.delete(esData);
                    //从表
                } else {
                    esData.setUpsert(false);
                    esTemplate.update(esData);
                }
                break;
            case NONE:
                break;
        }
    }
}
