package com.alibaba.otter.canal.client.adapter.es.support;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.alibaba.otter.canal.client.adapter.es.support.handler.DataMappingHandlerFactory;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.google.common.base.Splitter;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * ES 同步工具同类
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncUtil {

    private static Logger logger = LoggerFactory.getLogger(ESSyncUtil.class);

    public static Object convertToEsObj(Object val, String fieldInfo) {
        if (val == null) {
            return null;
        }
        if (fieldInfo.startsWith("array:")) {
            String separator = fieldInfo.substring("array:".length()).trim();
            String[] values = val.toString().split(separator);
            return Arrays.asList(values);
        } else if (fieldInfo.startsWith("object")) {
            if (val instanceof String) {
                return JSON.parse(val.toString());
            }
            return JSON.parse(new String((byte[]) val));
        }
        return null;
    }


    /**
     * 类型转换为Mapping中对应的类型
     */
    public static Object dataMapping(Object val, String esType) {
        if (val == null) {
            return null;
        }
        if (esType == null) {
            return val;
        }
        Object res = null;
        switch (esType) {
            case "integer":
                if (val instanceof Number) {
                    res = ((Number) val).intValue();
                } else {
                    res = Integer.parseInt(val.toString());
                }
                break;
            case "long":
                if (val instanceof Number) {
                    res = ((Number) val).longValue();
                } else {
                    res = Long.parseLong(val.toString());
                }
                break;
            case "short":
                if (val instanceof Number) {
                    res = ((Number) val).shortValue();
                } else {
                    res = Short.parseShort(val.toString());
                }
                break;
            case "byte":
                if (val instanceof Number) {
                    res = ((Number) val).byteValue();
                } else {
                    res = Byte.parseByte(val.toString());
                }
                break;
            case "double":
                if (val instanceof Number) {
                    res = ((Number) val).doubleValue();
                } else {
                    res = Double.parseDouble(val.toString());
                }
                break;
            case "float":
            case "half_float":
            case "scaled_float":
                if (val instanceof Number) {
                    res = ((Number) val).floatValue();
                } else {
                    res = Float.parseFloat(val.toString());
                }
                break;
            case "boolean":
                if (val instanceof Boolean) {
                    res = val;
                } else if (val instanceof Number) {
                    int v = ((Number) val).intValue();
                    res = v != 0;
                } else {
                    res = Boolean.parseBoolean(val.toString());
                }
                break;
            case "date":
                if (val instanceof java.sql.Time) {
                    DateTime dateTime = new DateTime(((java.sql.Time) val).getTime());
                    if (dateTime.getMillisOfSecond() != 0) {
                        res = dateTime.toString("HH:mm:ss.SSS");
                    } else {
                        res = dateTime.toString("HH:mm:ss");
                    }
                } else if (val instanceof java.sql.Timestamp) {
                    DateTime dateTime = new DateTime(((java.sql.Timestamp) val).getTime());
                    if (dateTime.getMillisOfSecond() != 0) {
                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
                    } else {
                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
                    }
                } else if (val instanceof java.sql.Date || val instanceof Date) {
                    DateTime dateTime;
                    if (val instanceof java.sql.Date) {
                        dateTime = new DateTime(((java.sql.Date) val).getTime());
                    } else {
                        dateTime = new DateTime(((Date) val).getTime());
                    }
                    if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
                            && dateTime.getMillisOfSecond() == 0) {
                        res = dateTime.toString("yyyy-MM-dd");
                    } else {
                        if (dateTime.getMillisOfSecond() != 0) {
                            res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
                        } else {
                            res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
                        }
                    }
                } else if (val instanceof Long) {
                    DateTime dateTime = new DateTime(((Long) val).longValue());
                    if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
                            && dateTime.getMillisOfSecond() == 0) {
                        res = dateTime.toString("yyyy-MM-dd");
                    } else if (dateTime.getMillisOfSecond() != 0) {
                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
                    } else {
                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
                    }
                } else if (val instanceof String) {
                    String v = ((String) val).trim();
                    if (v.length() > 18 && v.charAt(4) == '-' && v.charAt(7) == '-' && v.charAt(10) == ' '
                            && v.charAt(13) == ':' && v.charAt(16) == ':') {
                        String dt = v.substring(0, 10) + "T" + v.substring(11);
                        Date date = Util.parseDate(dt);
                        if (date != null) {
                            DateTime dateTime = new DateTime(date);
                            if (dateTime.getMillisOfSecond() != 0) {
                                res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
                            } else {
                                res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
                            }
                        }
                    } else if (v.length() == 10 && v.charAt(4) == '-' && v.charAt(7) == '-') {
                        Date date = Util.parseDate(v);
                        if (date != null) {
                            DateTime dateTime = new DateTime(date);
                            res = dateTime.toString("yyyy-MM-dd");
                        }
                    }
                }
                break;
            case "binary":
                if (val instanceof byte[]) {
                    Base64 base64 = new Base64();
                    res = base64.encodeAsString((byte[]) val);
                } else if (val instanceof Blob) {
                    byte[] b = blobToBytes((Blob) val);
                    Base64 base64 = new Base64();
                    res = base64.encodeAsString(b);
                } else if (val instanceof String) {
                    // 对应canal中的单字节编码
                    byte[] b = ((String) val).getBytes(StandardCharsets.ISO_8859_1);
                    Base64 base64 = new Base64();
                    res = base64.encodeAsString(b);
                }
                break;
            case "geo_point":
                if (!(val instanceof String)) {
                    logger.error("es type is geo_point, but source type is not String");
                    return val;
                }

                if (!((String) val).contains(",")) {
                    logger.error("es type is geo_point, source value not contains ',' separator");
                    return val;
                }

                String[] point = ((String) val).split(",");
                Map<String, Double> location = new HashMap<>();
                location.put("lat", Double.valueOf(point[0].trim()));
                location.put("lon", Double.valueOf(point[1].trim()));
                return location;
            case "array":
                if ("".equals(val.toString().trim())) {
                    res = new ArrayList<>();
                } else {
                    String value = val.toString();
                    String separator = ",";
                    if (!value.contains(",")) {
                        if (value.contains(";")) {
                            separator = ";";
                        } else if (value.contains("|")) {
                            separator = "\\|";
                        } else if (value.contains("-")) {
                            separator = "-";
                        }
                    }
                    String[] values = value.split(separator);
                    return Arrays.asList(values);
                }
                break;
            case "object":
                if ("".equals(val.toString().trim())) {
                    res = new HashMap<>();
                } else {
                    res = JSON.parseObject(val.toString(), Map.class);
                }
                break;
            default:
                // 其他类全以字符串处理
                res = val.toString();
                break;
        }

        return res;
    }

    /**
     * Blob转byte[]
     */
    private static byte[] blobToBytes(Blob blob) {
        try (InputStream is = blob.getBinaryStream()) {
            byte[] b = new byte[(int) blob.length()];
            if (is.read(b) != -1) {
                return b;
            } else {
                return new byte[0];
            }
        } catch (IOException | SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    /**
     * @param sourceData
     * @param fieldMapping
     * @param esField
     * @return java.lang.Object
     * @description 数据映射, 仅布尔和二进制的基础类型, 其余基础类型一律按字符串解析, es会自动转换.
     * 自定义映射规则依靠自定义的解析器实现.
     * @author 黄念
     * @date 2020/12/25 8:52
     */
    public static Object dataMapping(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, String esField) {
        //取得对应的sql字段. 如果对应的sql字段为空,则使用默认的规则获取sql字段. 例: userName->user_name
        String sqlField = fieldMapping.getColumn() != null ? fieldMapping.getColumn() : getDefaultSqlField(esField);

        //如果字段的数据处理器为空,直接从源数据中取值.
        if (fieldMapping.getHandler() == null) return sourceData.get(sqlField);

        //基本类型除了布尔类型,其他的基本类型不需要额外转换.
        Object val = sourceData.get(sqlField);
        switch (fieldMapping.getHandler()) {
            case "boolean":
                if (val == null) return null;
                return ((Byte) val).intValue() != 0;

            case "binary":
                if (val == null) return null;
                if (val instanceof byte[]) {
                    Base64 base64 = new Base64();
                    return base64.encodeAsString((byte[]) val);

                } else if (val instanceof Blob) {
                    byte[] b = blobToBytes((Blob) val);
                    Base64 base64 = new Base64();
                    return base64.encodeAsString(b);

                } else if (val instanceof String) {
                    // 对应canal中的单字节编码
                    byte[] b = ((String) val).getBytes(StandardCharsets.ISO_8859_1);
                    Base64 base64 = new Base64();
                    return base64.encodeAsString(b);
                }
                //自定义数据转换器
            default:
                return DataMappingHandlerFactory.getInstance(fieldMapping.getHandler()).handle(sourceData, fieldMapping);
        }
    }

    private static String getDefaultSqlField(String str) {
        return str.replaceAll("[A-Z]", '_' + "$0").toLowerCase();
    }


    public static List<Integer> strToIntList(String s) {
        return strToIntList(s, ",");
    }

    public static List<Integer> strToIntList(String str, String separator) {
        if (StringUtils.isBlank(str)) return new ArrayList<>();
        return strToList(str, separator).stream().map(Integer::parseInt).collect(Collectors.toList());
    }

    public static List<String> strToList(String str) {
        if (StringUtils.isBlank(str)) return new ArrayList<>();
        return strToList(str, ",");
    }

    public static List<String> strToList(String str, String separator) {
        if (StringUtils.isBlank(str)) return new ArrayList<>();
        return Splitter.on(separator).trimResults().omitEmptyStrings().splitToList(str);
    }


    public static String[] strToArray(String str) {
        return strToArray(str, ",");
    }


    public static String[] strToArray(String str, String separator) {
        if (str == null || str.equals("") || str.trim().equals("")) throw new RuntimeException();
        return str.split(separator);
    }


}
