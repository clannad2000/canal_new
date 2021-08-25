package com.alibaba.otter.canal.client.adapter.es.service;

import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.load.Loader;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/22
 * @Version1.0
 */
public class ESSyncServiceFactory {
    private static Map<Class<? extends IESSyncService>, IESSyncService> syncServiceMap = new HashMap<>();

    public static IESSyncService getInstant(Class<?> cls) {
        return syncServiceMap.get(cls);
    }

    public static void init(ESTemplate esTemplate, Loader loader) {
        syncServiceMap.put(CanalESSyncService.class, new CanalESSyncService(esTemplate, loader));
        syncServiceMap.put(EtlESSyncService.class, new EtlESSyncService(esTemplate, loader));
    }

}
