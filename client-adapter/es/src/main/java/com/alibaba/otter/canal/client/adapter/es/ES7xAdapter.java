package com.alibaba.otter.canal.client.adapter.es;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.es.etl.ESEtlService;
import com.alibaba.otter.canal.client.adapter.es.monitor.ESConfigMonitor;
import com.alibaba.otter.canal.client.adapter.es.service.ESSyncService;
import com.alibaba.otter.canal.client.adapter.es.support.model.DmlFilterConfig;
import com.alibaba.otter.canal.client.adapter.es.support.ES7xTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.ESConnection;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es.support.transform.data.DataHandlerFactory;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ES 7.x 外部适配器
 *
 * @author rewerma 2019-09-23
 * @version 1.0.0
 */
@SPI("es")
public class ES7xAdapter implements OuterAdapter {

    private ESConnection esConnection;

    public ESConnection getEsConnection() {
        return esConnection;
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            DataHandlerFactory.init();

            Map<String, String> properties = configuration.getProperties();

            String[] hostArray = configuration.getHosts().split(",");
            String mode = properties.get("mode");
            if ("rest".equalsIgnoreCase(mode) || "http".equalsIgnoreCase(mode)) {
                esConnection = new ESConnection(hostArray, properties);
            }
            this.esTemplate = new ES7xTemplate(esConnection);

            envProperties.put("es.version", "es");


            try {
                this.envProperties = envProperties;
                Map<String, ESSyncConfig> esSyncConfigTmp = ESSyncConfigLoader.load(envProperties);
                // 过滤不匹配的key的配置
                esSyncConfigTmp.forEach((key, config) -> {
                    if ((config.getOuterAdapterKey() == null && configuration.getKey() == null)
                            || (config.getOuterAdapterKey() != null && config.getOuterAdapterKey()
                            .equalsIgnoreCase(configuration.getKey()))) {
                        esSyncConfig.put(key, config);
                    }
                });

                for (Map.Entry<String, ESSyncConfig> entry : esSyncConfig.entrySet()) {
                    String configName = entry.getKey();
                    ESSyncConfig config = entry.getValue();

                    addSyncConfigToCache(configName, config);
                }

                dmlFilterConfig = builderFilterConfig();

                esSyncService = new ESSyncService(esTemplate);

                esConfigMonitor = new ESConfigMonitor();
                esConfigMonitor.init(this, envProperties);
            } catch (Throwable e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SneakyThrows
    public Map<String, Object> count(String task) {
        ESSyncConfig config = esSyncConfig.get(task);
        ESSyncConfig.ESMapping mapping = config.getEsMapping();

        long rowCount = esConnection.getRestHighLevelClient()
                .search(new SearchRequest(mapping.get_index()), RequestOptions.DEFAULT)
                .getHits().getTotalHits().value;
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("esIndex", mapping.get_index());
        res.put("count", rowCount);
        return res;
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            ESEtlService esEtlService = new ESEtlService(esConnection, config);
            if (dataSource != null) {
                return esEtlService.importData(params);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSuccess = true;
            for (ESSyncConfig configTmp : esSyncConfig.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    ESEtlService esEtlService = new ESEtlService(esConnection, configTmp);
                    EtlResult etlRes = esEtlService.importData(params);
                    if (!etlRes.getSucceeded()) {
                        resSuccess = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSuccess);
                if (resSuccess) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    @Override
    public void destroy() {
        if (esConfigMonitor != null) {
            esConfigMonitor.destroy();
        }
        if (esConnection != null) {
            esConnection.close();
        }
    }


    protected Map<String, ESSyncConfig> esSyncConfig = new ConcurrentHashMap<>(); // 文件名对应配置
    protected Map<String, Map<String, ESSyncConfig>> dbTableEsSyncConfig = new ConcurrentHashMap<>(); // schema-table对应配置

    protected ESTemplate esTemplate;

    protected ESSyncService esSyncService;

    protected ESConfigMonitor esConfigMonitor;

    protected Properties envProperties;

    protected List<DmlFilterConfig> dmlFilterConfig;

    public ESSyncService getEsSyncService() {
        return esSyncService;
    }

    public Map<String, ESSyncConfig> getEsSyncConfig() {
        return esSyncConfig;
    }

    public Map<String, Map<String, ESSyncConfig>> getDbTableEsSyncConfig() {
        return dbTableEsSyncConfig;
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        for (Dml dml : dmls) {
            if (!dml.getIsDdl()) {
                filterDml(dml);
                sync(dml);
            }
        }
        esSyncService.commit(); // 批次统一提交
    }


    public void filterDml(Dml dml) {
        Set<Object> set = new HashSet<>();
        dmlFilterConfig.forEach(dmlFilterConfig -> {
            if (dmlFilterConfig.getTableName().equals(dml.getTable())) {
                for (int i = 0; i < dml.getData().size(); i++) {
                    set.add(dml.getData().get(i).get(dmlFilterConfig.getIdColumn()));
                }

                if (set.size() > 1) return;
                Map<String, Object> data = dml.getData().get(0);
                dml.getData().clear();
                dml.getData().add(data);
                if (dml.getOld() != null) {
                    Map<String, Object> old = dml.getOld().get(0);
                    dml.getOld().clear();
                    dml.getOld().add(old);
                }
            }
        });
    }

    public List<DmlFilterConfig> builderFilterConfig() {
        List<DmlFilterConfig> list = new ArrayList<>();
        esSyncConfig.values().forEach(config -> {
            if (config.getEsMapping().isDmlFilter()) {
                list.add(DmlFilterConfig.builder()
                        .tableName(config.getEsMapping().getTableName())
                        .idColumn(config.getEsMapping().getProperties().get(config.getEsMapping().get_id()).getColumn())
                        .build());
            }
        });
        return list;
    }

    private void sync(Dml dml) {
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> configMap;
        if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            configMap = dbTableEsSyncConfig.get(StringUtils.trimToEmpty(dml.getDestination()) + "-"
                    + StringUtils.trimToEmpty(dml.getGroupId()) + "_" + database + "-"
                    + table);
        } else {
            configMap = dbTableEsSyncConfig.get(StringUtils.trimToEmpty(dml.getDestination()) + "_" + database + "-"
                    + table);
        }

        if (configMap != null && !configMap.values().isEmpty()) {
            esSyncService.sync(configMap.values(), dml);
        } else System.out.println("Not sync config found: " + dml);
    }


    @Override
    public String getDestination(String task) {
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }

    @SneakyThrows
    public void addSyncConfigToCache(String configName, ESSyncConfig config) {
        Properties envProperties = this.envProperties;
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (dataSource == null || dataSource.getUrl() == null) {
            throw new RuntimeException("No data source found: " + config.getDataSourceKey());
        }
        Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
        Matcher matcher = pattern.matcher(dataSource.getUrl());
        if (!matcher.find()) {
            throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
        }
        String schema = matcher.group(2);

        String tableName = config.getEsMapping().getTableName();
        Map<String, ESSyncConfig> esSyncConfigMap;
        if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
            esSyncConfigMap = dbTableEsSyncConfig.computeIfAbsent(StringUtils.trimToEmpty(config.getDestination())
                            + "-"
                            + StringUtils.trimToEmpty(config.getGroupId())
                            + "_"
                            + schema
                            + "-"
                            + tableName,
                    k -> new ConcurrentHashMap<>());
        } else {
            esSyncConfigMap = dbTableEsSyncConfig.computeIfAbsent(StringUtils.trimToEmpty(config.getDestination())
                            + "_"
                            + schema
                            + "-"
                            + tableName,
                    k -> new ConcurrentHashMap<>());
        }

        esSyncConfigMap.put(configName, config);
    }
}
