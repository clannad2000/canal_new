package com.alibaba.otter.canal.client.adapter.support;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/7/14
 * @Version1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlatDml implements Serializable {
        private static final long         serialVersionUID = -1L;

        private String                    destination;                            // 对应canal的实例或者MQ的topic
        private String                    groupId;                                // 对应mq的group id
        private String                    database;                               // 数据库或schema
        private String                    table;                                  // 表名
        private List<String> pkNames;
        private Boolean                   isDdl;
        private OpTypeEnum                type;                                   // 类型: INSERT UPDATE DELETE
        // binlog executeTime
        private Long                      es;                                     // 执行耗时
        // dml build timeStamp
        private Long                      ts;                                     // 同步时间
        private String                    sql;                                    // 执行的sql, dml sql为空
        private Map<String, Object>       data;                                   // 数据
        private Map<String, Object>       old;                                    // 旧数据, 用于update, size和data的size一一对应


    public void clear() {
        database = null;
        table = null;
        type = null;
        ts = null;
        es = null;
        data = null;
        old = null;
        sql = null;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public void setData(Map<String, Object> data, String id) {
        if (data != null) {
            Object pkVal = data.get(id);
            this.data = data;
            this.data.put(id, pkVal);
        }
    }

    @Override
        public String toString() {
        return "Dml{" + "destination='" + destination + '\'' + ", database='" + database + '\'' + ", table='" + table
                + '\'' + ", type='" + type + '\'' + ", es=" + es + ", ts=" + ts + ", sql='" + sql + '\'' + ", data="
                + data + ", old=" + old + '}';
    }
}

