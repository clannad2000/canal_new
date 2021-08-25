package com.alibaba.otter.canal.client.adapter.es.support.transformer.processor;

import com.alibaba.otter.canal.client.adapter.es.support.emun.ParamsSrcType;
import com.alibaba.otter.canal.client.adapter.es.support.mapper.MapperFactory;
import com.alibaba.otter.canal.client.adapter.es.support.model.ESData;
import com.alibaba.otter.canal.client.adapter.es.support.model.MapperContext;
import com.alibaba.otter.canal.client.adapter.es.support.model.TransformContext;
import com.alibaba.otter.canal.client.adapter.es.support.transformer.AbstractTransformer;
import com.alibaba.otter.canal.client.adapter.support.OpTypeEnum;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.stereotype.Service;


/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/15
 * @Version1.0
 */
@Service
public class UpdateByQueryExample extends AbstractProcessorTransformer {


    @Override
    protected void preprocess(ESData esData, TransformContext context) {
        esData.setDstOpType(OpTypeEnum.UPDATE_BY_QUERY);
        esData.setParamsSrc(ParamsSrcType.PARAMS);
        esData.setScript("tyre-sku-s_product_sku_area");
        esData.setQuery(QueryBuilders.termsQuery("skuId", esData.getFlatDml().getData().get("sku_id")));
        MapperFactory.getInstance().mapping(esData, MapperContext.builder().esMapping(context.getEsMapping()).build());
        esData.setParams(esData.getEsFieldData());
    }

    @Override
    protected void insert(ESData esData, TransformContext context) {

    }

    @Override
    protected void update(ESData esData, TransformContext context) {

    }

    @Override
    protected void delete(ESData esData, TransformContext context) {

    }
}

