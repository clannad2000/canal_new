package com.alibaba.otter.canal.client.adapter.support;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * DML操作转换对象
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Dml implements Serializable {

    private static final long         serialVersionUID = 2611556444074013268L;

    private String                    destination;                            // 对应canal的实例或者MQ的topic
    private String                    groupId;                                // 对应mq的group id
    private String                    database;                               // 数据库或schema
    private String                    table;                                  // 表名
    private List<String>              pkNames;
    private Boolean                   isDdl;
    private String                    type;                                   // 类型: INSERT UPDATE DELETE
    // binlog executeTime
    private Long                      es;                                     // 执行耗时
    // dml build timeStamp
    private Long                      ts;                                     // 同步时间
    private String                    sql;                                    // 执行的sql, dml sql为空
    private List<Map<String, Object>> data;                                   // 数据列表
    private List<Map<String, Object>> old;                                    // 旧数据列表, 用于update, size和data的size一一对应
    private Integer                   index;

//    public Map<String, Object> findData(String table,String fieldName,Object fieldVal){
//        for (Map<String, Object> datum : this.data) {
//            Object val = datum.get(fieldName);
//            if(val!=null&&val.toString().equalsIgnoreCase(fieldVal.toString()))return datum;
//        }
//        return null;
//    }

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

    @Override
    public String toString() {
        return "Dml{" + "destination='" + destination + '\'' + ", database='" + database + '\'' + ", table='" + table
               + '\'' + ", type='" + type + '\'' + ", es=" + es + ", ts=" + ts + ", sql='" + sql + '\'' + ", data="
               + data + ", old=" + old + '}';
    }
}
