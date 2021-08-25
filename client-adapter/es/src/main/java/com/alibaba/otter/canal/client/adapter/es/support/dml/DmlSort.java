package com.alibaba.otter.canal.client.adapter.es.support.dml;

import com.alibaba.otter.canal.client.adapter.es.support.model.Config;
import com.alibaba.otter.canal.client.adapter.es.support.model.OrderGroupContext;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.google.common.collect.TreeMultimap;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/19
 * @Version1.0
 */
public class DmlSort {
    //      * 配置:
    //        *   表和表的顺序 列表
    //        * 一个list记录同事务时间的数据
    //        *   配置,索引和对应的表名,
    //        *   根据配置重排顺序
    private static List<Dml> testDmls = new ArrayList<>();

    private static Map<String, List<Config>> testConfigs = new HashMap<>();

    private Map<String, List<Config>> configs = new HashMap<>();

    public DmlSort(Map<String, List<Config>> configs) {
        this.configs = configs;
    }

    static {

        testDmls.add(Dml.builder().table("s_product_sku_detail").es(1L).build());

        testDmls.add(Dml.builder().table("s_company").es(2L).build());
        testDmls.add(Dml.builder().table("s_product_sku").es(2L).build());

        testDmls.add(Dml.builder().table("s_company").es(1L).build());
        testDmls.add(Dml.builder().table("s_product_sku").es(1L).build());
        testDmls.add(Dml.builder().table("s_product_sku_area").es(1L).build());


        testConfigs.put("company", Arrays.asList(
                Config.builder().groupName("company").table("s_product_sku").order(1).build(),
                Config.builder().groupName("company").table("s_company").order(2).build(),
                Config.builder().groupName("company").table("s_product_sku_area").order(3).build()
        ));
    }


    public static void main(String[] args) {
        new DmlSort(testConfigs).reorder(new ArrayList<>(), dml -> {
            return true;
        });
    }

    public void reorder(List<Dml> dmls, Function<Dml, Boolean> filter) {
        for (int i = 0; i < dmls.size(); i++) {
            dmls.get(i).setIndex(i);
        }

        //构建上下文
        List<OrderGroupContext> contextList = new ArrayList<>();
        for (int i = 0; i < dmls.size(); i++) {
            filter.apply(dmls.get(i));
            List<Config> configList = getConfigs(dmls.get(i).getTable());
            for (Config config : configList) {
                OrderGroupContext orderGroupContext = OrderGroupContext.builder()
                        .es(dmls.get(i).getEs())
                        .groupName(config.getGroupName())
                        .groupSize(configs.get(config.getGroupName()).size())
                        .tableName(config.getTable())
                        .order(config.getOrder())
                        .index(i)
                        .build();
                contextList.add(orderGroupContext);
            }
        }


        TreeMultimap<String, OrderGroupContext> multimap = TreeMultimap.create((o1, o2) -> o1.equals(o2) ? 0 : 1, Comparator.comparingInt(OrderGroupContext::getOrder));

        //分组
        for (OrderGroupContext orderGroupContext : contextList) {
            multimap.put(orderGroupContext.getGroupName() + orderGroupContext.getEs(), orderGroupContext);
        }

        //重排
        for (String key : multimap.keySet()) {
            NavigableSet<OrderGroupContext> orderGroupContexts = multimap.get(key);
            if (orderGroupContexts.size() != orderGroupContexts.first().getGroupSize()) continue;
            List<Dml> dmlList = new ArrayList<>(orderGroupContexts.size());

            for (OrderGroupContext orderGroupContext : orderGroupContexts) {
                dmlList.add(dmls.get(orderGroupContext.getIndex()));
            }

            List<Integer> list = orderGroupContexts.stream().map(OrderGroupContext::getIndex).sorted(Integer::compareTo).collect(Collectors.toList());
            for (int i = 0; i < list.size(); i++) {
                dmls.set(list.get(i), dmlList.get(i));
            }
        }

        System.out.println(dmls);
    }


    //根据表名获取组列表
    public List<Config> getConfigs(String table) {
        List<Config> list = new ArrayList<>();
        for (List<Config> value : configs.values()) {
            for (Config config : value) {
                if (config.getTable().equalsIgnoreCase(table)) list.add(config);
            }
        }
        return list;
    }
}
