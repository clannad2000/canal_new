package com.alibaba.otter.canal.client.adapter.es.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.es.support.processor.post.Postprocessor;
import com.alibaba.otter.canal.client.adapter.es.support.processor.updateByQuery.UpdateByQueryBuilder;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import lombok.Data;

/**
 * ES 映射配置
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
@Data
public class ESSyncConfig implements AdapterConfig {

    private String dataSourceKey;    // 数据源key

    private String outerAdapterKey;  // adapter key

    private String groupId;          // group id

    private String destination;      // canal destination

    private ESMapping esMapping;

    private String esVersion = "es";

    public void validate() {
        if (esMapping._index == null) {
            throw new NullPointerException("esMapping._index");
        }
        if ("es6".equals(esVersion) && esMapping._type == null) {
            throw new NullPointerException("esMapping._type");
        }
        if (esMapping._id == null) {
            throw new NullPointerException("esMapping._id or esMapping.pk");
        }

        if (esMapping.postprocessor && Postprocessor.getInstance(esMapping.configFileName) == null)
            throw new NullPointerException("Postprocessor");

        if (esMapping.updateByQuery && UpdateByQueryBuilder.getInstance(esMapping.configFileName) == null)
            throw new NullPointerException("UpdateByQueryBuilder");

    }

    public ESMapping getEsMapping() {
        return esMapping;
    }

    @Override
    public AdapterMapping getMapping() {
        return esMapping;
    }

    @Data
    public static class ESMapping implements AdapterMapping {

        private String _index;
        private String _type;
        private String _id;
        private boolean upsert = false;
        private Map<String, RelationMapping> relations = new LinkedHashMap<>();
        private String sql;
        // 对象字段, 例: objFields:
        // - _labels: array:;
        private Map<String, String> objFields = new LinkedHashMap<>();
        private List<String> skips = new ArrayList<>();
        private int commitBatch = 1000;
        private String etlCondition;
        private boolean syncByTimestamp = false;                // 是否按时间戳定时同步
        private Long syncInterval;                           // 同步时间间隔

        private boolean main; //是否是主表
        private boolean idMode; //主键: true: id模式, false: pk
        private String tableName; //数据库表名
        private Map<String, FieldMapping> properties = new LinkedHashMap<>(); //es属性表

        private boolean postprocessor = false; //是否开启后置处理器

        private boolean updateByQuery = false;  //是否开启查询更新

        private String configFileName;  //配置文件名

        public void set_id(String _id) {
            this._id = _id;
            if ("_id".equalsIgnoreCase(_id)) idMode = true;
        }


        @Data
        public static class FieldMapping {
            private String processor; //处理器名称
            private String column; //数据来源的sql列名
            private String param; //参数
            private String dataSourceKey;
            private String sql;
        }


        @Data
        public static class UpdateByQueryMapping {
            private String dataOrigin; //数据来源
            private String dataSourceKey;
            private String sql;
            private String param; //参数
            private String scriptId; //es脚本id
        }
    }


    @Data
    public static class RelationMapping {
        private String name;
        private String parent;
    }
}
