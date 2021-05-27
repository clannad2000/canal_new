package com.alibaba.otter.canal.client.adapter.es.support;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import com.alibaba.otter.canal.client.adapter.es.support.emun.OperationEnum;
import com.alibaba.otter.canal.client.adapter.es.support.processor.data.FieldMappingProcessorFactory;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.google.common.base.Splitter;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

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


//    /**
//     * 类型转换为Mapping中对应的类型
//     */
//    public static Object dataMapping(Object val, String esType) {
//        if (val == null) {
//            return null;
//        }
//        if (esType == null) {
//            return val;
//        }
//        Object res = null;
//        switch (esType) {
//            case "integer":
//                if (val instanceof Number) {
//                    res = ((Number) val).intValue();
//                } else {
//                    res = Integer.parseInt(val.toString());
//                }
//                break;
//            case "long":
//                if (val instanceof Number) {
//                    res = ((Number) val).longValue();
//                } else {
//                    res = Long.parseLong(val.toString());
//                }
//                break;
//            case "short":
//                if (val instanceof Number) {
//                    res = ((Number) val).shortValue();
//                } else {
//                    res = Short.parseShort(val.toString());
//                }
//                break;
//            case "byte":
//                if (val instanceof Number) {
//                    res = ((Number) val).byteValue();
//                } else {
//                    res = Byte.parseByte(val.toString());
//                }
//                break;
//            case "double":
//                if (val instanceof Number) {
//                    res = ((Number) val).doubleValue();
//                } else {
//                    res = Double.parseDouble(val.toString());
//                }
//                break;
//            case "float":
//            case "half_float":
//            case "scaled_float":
//                if (val instanceof Number) {
//                    res = ((Number) val).floatValue();
//                } else {
//                    res = Float.parseFloat(val.toString());
//                }
//                break;
//            case "boolean":
//                if (val instanceof Boolean) {
//                    res = val;
//                } else if (val instanceof Number) {
//                    int v = ((Number) val).intValue();
//                    res = v != 0;
//                } else {
//                    res = Boolean.parseBoolean(val.toString());
//                }
//                break;
//            case "date":
//                if (val instanceof java.sql.Time) {
//                    DateTime dateTime = new DateTime(((java.sql.Time) val).getTime());
//                    if (dateTime.getMillisOfSecond() != 0) {
//                        res = dateTime.toString("HH:mm:ss.SSS");
//                    } else {
//                        res = dateTime.toString("HH:mm:ss");
//                    }
//                } else if (val instanceof java.sql.Timestamp) {
//                    DateTime dateTime = new DateTime(((java.sql.Timestamp) val).getTime());
//                    if (dateTime.getMillisOfSecond() != 0) {
//                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
//                    } else {
//                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
//                    }
//                } else if (val instanceof java.sql.Date || val instanceof Date) {
//                    DateTime dateTime;
//                    if (val instanceof java.sql.Date) {
//                        dateTime = new DateTime(((java.sql.Date) val).getTime());
//                    } else {
//                        dateTime = new DateTime(((Date) val).getTime());
//                    }
//                    if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
//                            && dateTime.getMillisOfSecond() == 0) {
//                        res = dateTime.toString("yyyy-MM-dd");
//                    } else {
//                        if (dateTime.getMillisOfSecond() != 0) {
//                            res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
//                        } else {
//                            res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
//                        }
//                    }
//                } else if (val instanceof Long) {
//                    DateTime dateTime = new DateTime(((Long) val).longValue());
//                    if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
//                            && dateTime.getMillisOfSecond() == 0) {
//                        res = dateTime.toString("yyyy-MM-dd");
//                    } else if (dateTime.getMillisOfSecond() != 0) {
//                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
//                    } else {
//                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
//                    }
//                } else if (val instanceof String) {
//                    String v = ((String) val).trim();
//                    if (v.length() > 18 && v.charAt(4) == '-' && v.charAt(7) == '-' && v.charAt(10) == ' '
//                            && v.charAt(13) == ':' && v.charAt(16) == ':') {
//                        String dt = v.substring(0, 10) + "T" + v.substring(11);
//                        Date date = Util.parseDate(dt);
//                        if (date != null) {
//                            DateTime dateTime = new DateTime(date);
//                            if (dateTime.getMillisOfSecond() != 0) {
//                                res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
//                            } else {
//                                res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
//                            }
//                        }
//                    } else if (v.length() == 10 && v.charAt(4) == '-' && v.charAt(7) == '-') {
//                        Date date = Util.parseDate(v);
//                        if (date != null) {
//                            DateTime dateTime = new DateTime(date);
//                            res = dateTime.toString("yyyy-MM-dd");
//                        }
//                    }
//                }
//                break;
//            case "binary":
//                if (val instanceof byte[]) {
//                    Base64 base64 = new Base64();
//                    res = base64.encodeAsString((byte[]) val);
//                } else if (val instanceof Blob) {
//                    byte[] b = blobToBytes((Blob) val);
//                    Base64 base64 = new Base64();
//                    res = base64.encodeAsString(b);
//                } else if (val instanceof String) {
//                    // 对应canal中的单字节编码
//                    byte[] b = ((String) val).getBytes(StandardCharsets.ISO_8859_1);
//                    Base64 base64 = new Base64();
//                    res = base64.encodeAsString(b);
//                }
//                break;
//            case "geo_point":
//                if (!(val instanceof String)) {
//                    logger.error("es type is geo_point, but source type is not String");
//                    return val;
//                }
//
//                if (!((String) val).contains(",")) {
//                    logger.error("es type is geo_point, source value not contains ',' separator");
//                    return val;
//                }
//
//                String[] point = ((String) val).split(",");
//                Map<String, Double> location = new HashMap<>();
//                location.put("lat", Double.valueOf(point[0].trim()));
//                location.put("lon", Double.valueOf(point[1].trim()));
//                return location;
//            case "array":
//                if ("".equals(val.toString().trim())) {
//                    res = new ArrayList<>();
//                } else {
//                    String value = val.toString();
//                    String separator = ",";
//                    if (!value.contains(",")) {
//                        if (value.contains(";")) {
//                            separator = ";";
//                        } else if (value.contains("|")) {
//                            separator = "\\|";
//                        } else if (value.contains("-")) {
//                            separator = "-";
//                        }
//                    }
//                    String[] values = value.split(separator);
//                    return Arrays.asList(values);
//                }
//                break;
//            case "object":
//                if ("".equals(val.toString().trim())) {
//                    res = new HashMap<>();
//                } else {
//                    res = JSON.parseObject(val.toString(), Map.class);
//                }
//                break;
//            default:
//                // 其他类全以字符串处理
//                res = val.toString();
//                break;
//        }
//
//        return res;
//    }

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
    public static Object dataMapping(Map<String, Object> sourceData, ESSyncConfig.ESMapping.FieldMapping fieldMapping, String esField, OperationEnum operationEnum) {
        //取得对应的sql字段. 如果对应的sql字段为空,则使用默认的规则获取sql字段. 例: userName->user_name
        String sqlField = fieldMapping.getColumn() != null ? fieldMapping.getColumn() : getDefaultSqlField(esField);

        //如果字段的数据处理器为空,则直接从源数据中取值.
        if (fieldMapping.getProcessor() == null) return sourceData.get(sqlField);

        //基本类型除了布尔类型,其他的基本类型不需要额外转换.
        Object val = sourceData.get(sqlField);
        switch (fieldMapping.getProcessor()) {
            case "boolean":
                if (val == null) return null;
                if (val instanceof Boolean) return val;
                //return ((Byte) val).intValue() != 0;
                return (val.toString().getBytes()[val.toString().length() - 1] & 1) == 1;
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
            case "object":
                if (val == null) return null;
                else if ("".equals(val.toString().trim())) {
                    return new HashMap<>();
                } else {
                    return JSON.parseObject(val.toString(), Map.class);
                }
                //自定义数据转换器
            default:
                return FieldMappingProcessorFactory.getInstance(fieldMapping.getProcessor()).dispose(sourceData, fieldMapping, operationEnum);
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


    public static List<String> strToList(Object object) {
        if (object == null) return new ArrayList<>();
        return strToList(object.toString());
    }



    public static Set<Class<?>> getClasses(String pack) {

        // 第一个class类的集合
        Set<Class<?>> classes = new LinkedHashSet<Class<?>>();
        // 是否循环迭代
        boolean recursive = true;
        // 获取包的名字 并进行替换
        String packageName = pack;
        String packageDirName = packageName.replace('.', '/');
        // 定义一个枚举的集合 并进行循环来处理这个目录下的things
        Enumeration<URL> dirs;
        try {
            dirs = Thread.currentThread().getContextClassLoader().getResources(
                    packageDirName);
            // 循环迭代下去
            while (dirs.hasMoreElements()) {
                // 获取下一个元素
                URL url = dirs.nextElement();
                // 得到协议的名称
                String protocol = url.getProtocol();
                // 如果是以文件的形式保存在服务器上
                if ("file".equals(protocol)) {
                    //System.err.println("file类型的扫描");
                    // 获取包的物理路径
                    String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                    // 以文件的方式扫描整个包下的文件 并添加到集合中
                    findAndAddClassesInPackageByFile(packageName, filePath,
                            recursive, classes);
                } else if ("jar".equals(protocol)) {
                    // 如果是jar包文件
                    // 定义一个JarFile
                    //System.err.println("jar类型的扫描");
                    JarFile jar;
                    try {
                        // 获取jar
                        jar = ((JarURLConnection) url.openConnection())
                                .getJarFile();
                        // 从此jar包 得到一个枚举类
                        Enumeration<JarEntry> entries = jar.entries();
                        // 同样的进行循环迭代
                        while (entries.hasMoreElements()) {
                            // 获取jar里的一个实体 可以是目录 和一些jar包里的其他文件 如META-INF等文件
                            JarEntry entry = entries.nextElement();
                            String name = entry.getName();
                            // 如果是以/开头的
                            if (name.charAt(0) == '/') {
                                // 获取后面的字符串
                                name = name.substring(1);
                            }
                            // 如果前半部分和定义的包名相同
                            if (name.startsWith(packageDirName)) {
                                int idx = name.lastIndexOf('/');
                                // 如果以"/"结尾 是一个包
                                if (idx != -1) {
                                    // 获取包名 把"/"替换成"."
                                    packageName = name.substring(0, idx)
                                            .replace('/', '.');
                                }
                                // 如果可以迭代下去 并且是一个包
                                if ((idx != -1) || recursive) {
                                    // 如果是一个.class文件 而且不是目录
                                    if (name.endsWith(".class")
                                            && !entry.isDirectory()) {
                                        // 去掉后面的".class" 获取真正的类名
                                        String className = name.substring(
                                                packageName.length() + 1, name
                                                        .length() - 6);
                                        try {
                                            // 添加到classes
                                            classes.add(Class.forName(packageName + '.'
                                                    + className));
                                        } catch (ClassNotFoundException e) {
                                            // log
                                            // .error("添加用户自定义视图类错误 找不到此类的.class文件");
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        // log.error("在扫描用户定义视图时从jar包获取文件出错");
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return classes;
    }

    /**
     * 以文件的形式来获取包下的所有Class
     *
     * @param packageName
     * @param packagePath
     * @param recursive
     * @param classes
     */
    public static void findAndAddClassesInPackageByFile(String packageName,
                                                        String packagePath, final boolean recursive, Set<Class<?>> classes) {
        // 获取此包的目录 建立一个File
        File dir = new File(packagePath);
        // 如果不存在或者 也不是目录就直接返回
        if (!dir.exists() || !dir.isDirectory()) {
            // log.warn("用户定义包名 " + packageName + " 下没有任何文件");
            return;
        }
        // 如果存在 就获取包下的所有文件 包括目录
        File[] dirfiles = dir.listFiles(new FileFilter() {
            // 自定义过滤规则 如果可以循环(包含子目录) 或则是以.class结尾的文件(编译好的java类文件)
            public boolean accept(File file) {
                return (recursive && file.isDirectory())
                        || (file.getName().endsWith(".class"));
            }
        });
        // 循环所有文件
        for (File file : dirfiles) {
            // 如果是目录 则继续扫描
            if (file.isDirectory()) {
                findAndAddClassesInPackageByFile(packageName + "."
                                + file.getName(), file.getAbsolutePath(), recursive,
                        classes);
            } else {
                // 如果是java类文件 去掉后面的.class 只留下类名
                String className = file.getName().substring(0,
                        file.getName().length() - 6);
                try {
                    // 添加到集合中去
                    //classes.add(Class.forName(packageName + '.' + className));
                    //经过回复同学的提醒，这里用forName有一些不好，会触发static方法，没有使用classLoader的load干净
                    classes.add(Thread.currentThread().getContextClassLoader().loadClass(packageName + '.' + className));
                } catch (ClassNotFoundException e) {
                    // log.error("添加用户自定义视图类错误 找不到此类的.class文件");
                    e.printStackTrace();
                }
            }
        }
    }

}
