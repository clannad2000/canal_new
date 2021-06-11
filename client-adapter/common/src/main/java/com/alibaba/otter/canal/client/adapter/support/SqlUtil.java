package com.alibaba.otter.canal.client.adapter.support;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @Description
 * @Author 黄念
 * @Date 2021/6/4
 * @Version1.0
 */
public class SqlUtil {


    private static final Logger logger = LoggerFactory.getLogger(SqlUtil.class);


    public static Object queryForObject(String dataSourceKey, String sql, List<Object> params) {
        List<Object> list = new ArrayList<>();
        query(dataSourceKey, sql, params, resultSet -> {
            try {
                while (resultSet.next()) {
                    list.add(resultSet.getObject(1));
                }
            } catch (Exception e) {
                logger.error("executeSqlForObject has error, sql: {} ", sql);
                throw new RuntimeException(e);
            }
        });
        return list;
    }

    public static <T> T queryForCls(String dataSourceKey, String sql, List<Object> params, Class<T> cls) {
        try {
            T instance = cls.getConstructor().newInstance();
            System.out.println(Arrays.toString(cls.getDeclaredFields()));
            query(dataSourceKey, sql, params, resultSet -> {
                try {
                    while (resultSet.next()) {
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            String columnName = metaData.getColumnName(i);
                            Field field = cls.getDeclaredField(columnName);
                            Object object = ResultSet.class.getMethod("get" + field.getType().getSimpleName(), String.class).invoke(resultSet, columnName);
                            Method method = cls.getMethod("set" + (char) (columnName.charAt(0) - 32) + columnName.substring(1), field.getType());
                            method.invoke(instance, object);
                        }
                    }
                } catch (SQLException | NoSuchFieldException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            });
            return instance;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static List<Map<String, Object>> queryForMapList(String dataSourceKey, String sql, List<Object> params) {
        List<Map<String, Object>> mapList = new ArrayList<>();
        query(dataSourceKey, sql, params, resultSet -> {
            try {
                while (resultSet.next()) {
                    Map<String, Object> map = new LinkedHashMap<>();
                    ResultSetMetaData md = resultSet.getMetaData(); //获得结果集结构信息,元数据
                    int columnCount = md.getColumnCount();   //获得列数
                    for (int i = 1; i <= columnCount; i++) {
                        map.put(md.getColumnLabel(i), resultSet.getObject(i));
                    }
                    mapList.add(map);
                }
            } catch (Exception e) {
                logger.error("executeSqlForMapList has error, sql: {} ", sql);
                throw new RuntimeException(e);
            }
        });
        return mapList;
    }

    public static Map<String, Object> queryForMap(String dataSourceKey, String sql, List<Object> params) {
        Map<String, Object> map = new LinkedHashMap<>();
        query(dataSourceKey, sql, params, resultSet -> {
            try {
                if (resultSet.next()) {
                    ResultSetMetaData md = resultSet.getMetaData(); //获得结果集结构信息,元数据
                    int columnCount = md.getColumnCount();   //获得列数
                    for (int i = 1; i <= columnCount; i++) {
                        map.put(md.getColumnLabel(i), resultSet.getObject(i));
                    }
                }
            } catch (Exception e) {
                logger.error("executeSqlForMap has error, sql: {} ", sql);
                throw new RuntimeException(e);
            }
        });
        return map;
    }


    public static List<Object> queryForList(String dataSourceKey, String sql, List<Object> params) {
        List<Object> list = new ArrayList<>();
        query(dataSourceKey, sql, params, resultSet -> {
            try {
                while (resultSet.next()) {
                    list.add(resultSet.getObject(1));
                }
            } catch (Exception e) {
                logger.error("executeSqlForList has error, sql: {} ", sql);
                throw new RuntimeException(e);
            }
        });
        return list;
    }


    public static void query(String dataSourceKey, String sql, List<Object> params, Consumer<ResultSet> consumer) {
        DruidDataSource ds = DatasourceConfig.DATA_SOURCES.get(dataSourceKey);
        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement pstmt = conn
                    .prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                pstmt.setFetchSize(Integer.MIN_VALUE);
                if (params != null) {
                    for (int i = 0; i < params.size(); i++) {
                        pstmt.setObject(i + 1, params.get(i));
                    }
                }

                try (ResultSet resultSet = pstmt.executeQuery()) {
                    consumer.accept(resultSet);
                }
            }
        } catch (Exception e) {
            logger.error("executeSql has error, sql: {} ", sql);
            throw new RuntimeException(e);
        }
    }
}
