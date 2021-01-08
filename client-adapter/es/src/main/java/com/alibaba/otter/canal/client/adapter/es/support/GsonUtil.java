package com.alibaba.otter.canal.client.adapter.es.support;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;


public class GsonUtil {
    public static final Gson gson = new GsonBuilder().setLenient()
            .enableComplexMapKeySerialization()
            .serializeNulls()
            // .setPrettyPrinting()// 调教格式
            .disableHtmlEscaping()
            .create();


    public static <T> T fromJson(String str, TypeToken typeToken) {
        // TypeToken<ArrayList<String>> typeToken = new TypeToken<ArrayList<String>>() {};
        return gson.fromJson(str, typeToken.getType());
    }


    public static <T> T fromJson(String json, Class<T> classOfT) {
        return gson.fromJson(json, classOfT);
    }

}
