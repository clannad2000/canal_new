package com.alibaba.otter.canal.client.adapter.es.support.transform.data;

import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 数据处理器工厂类
 * @Description
 * @Author 黄念
 * @Date 2021/6/11
 * @Version1.0
 */
public class DataHandlerFactory {
    private static Map<String, DataHandler> DataHandlerMap = new ConcurrentHashMap<>();

    private static DefaultDataHandler defaultDataHandler = new DefaultDataHandler();

    public static void init() throws Exception {
        for (Class<?> cls : ESSyncUtil.getClasses(DataHandler.class.getPackage().getName())) {
            Service annotation = cls.getAnnotation(Service.class);
            if (annotation != null) {
                Constructor<?> constructor = cls.getConstructor();
                DataHandler DataHandler = (DataHandler) constructor.newInstance();
                DataHandlerMap.put(cls.getSimpleName().toLowerCase(), DataHandler);
            }
        }
    }

    public static DataHandler getDataHandler(String name) {
        DataHandler dataHandler = DataHandlerMap.get(name);
        return dataHandler != null ? dataHandler : defaultDataHandler;
    }

}
