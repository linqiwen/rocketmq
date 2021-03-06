/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class MixAll {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public static final String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    /**
     * rocketMq主目录
     */
    public static final String ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";
    public static final String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    public static final String NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
    public static final String MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel";
    public static final String DEFAULT_NAMESRV_ADDR_LOOKUP = "jmenv.tbsite.net";
    public static final String WS_DOMAIN_NAME = System.getProperty("rocketmq.namesrv.domain", DEFAULT_NAMESRV_ADDR_LOOKUP);
    public static final String WS_DOMAIN_SUBGROUP = System.getProperty("rocketmq.namesrv.domain.subgroup", "nsaddr");
    //http://jmenv.tbsite.net:8080/rocketmq/nsaddr
    //public static final String WS_ADDR = "http://" + WS_DOMAIN_NAME + ":8080/rocketmq/" + WS_DOMAIN_SUBGROUP;
    /**
     * 自动创建topic
     */
    public static final String AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102"; // Will be created at broker when isAutoCreateTopicEnable
    /**
     * 基准测试主题
     */
    public static final String BENCHMARK_TOPIC = "BenchmarkTest";
    public static final String DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
    public static final String DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";
    public static final String TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER";
    public static final String FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER";
    public static final String MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER";
    public static final String CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER";
    public static final String SELF_TEST_PRODUCER_GROUP = "SELF_TEST_P_GROUP";
    public static final String SELF_TEST_CONSUMER_GROUP = "SELF_TEST_C_GROUP";
    public static final String SELF_TEST_TOPIC = "SELF_TEST_TOPIC";
    /**
     * 偏移量移除事件
     */
    public static final String OFFSET_MOVED_EVENT = "OFFSET_MOVED_EVENT";
    public static final String ONS_HTTP_PROXY_GROUP = "CID_ONS-HTTP-PROXY";
    public static final String CID_ONSAPI_PERMISSION_GROUP = "CID_ONSAPI_PERMISSION";
    public static final String CID_ONSAPI_OWNER_GROUP = "CID_ONSAPI_OWNER";
    public static final String CID_ONSAPI_PULL_GROUP = "CID_ONSAPI_PULL";
    public static final String CID_RMQ_SYS_PREFIX = "CID_RMQ_SYS_";

    public static final List<String> LOCAL_INET_ADDRESS = getLocalInetAddress();
    public static final String LOCALHOST = localhost();
    /**
     * 默认编码，UTF-8
     */
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final long MASTER_ID = 0L;
    public static final long CURRENT_JVM_PID = getPID();

    /**
     * 重试组主题前缀
     */
    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";

    public static final String DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";
    public static final String SYSTEM_TOPIC_PREFIX = "rmq_sys_";
    public static final String UNIQUE_MSG_QUERY_FLAG = "_UNIQUE_KEY_QUERY";
    //默认的区域id
    public static final String DEFAULT_TRACE_REGION_ID = "DefaultRegion";
    public static final String CONSUME_CONTEXT_TYPE = "ConsumeContextType";

    /**
     * 半消息存放主题
     */
    public static final String RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC";
    /**
     * 系统跟踪topic
     */
    public static final String RMQ_SYS_TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC";
    /**
     * 已被处理半消息主题
     */
    public static final String RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    public static final String CID_SYS_RMQ_TRANS = "CID_RMQ_SYS_TRANS";
    public static final String ACL_CONF_TOOLS_FILE = "/conf/tools.yml";

    public static String getWSAddr() {
        String wsDomainName = System.getProperty("rocketmq.namesrv.domain", DEFAULT_NAMESRV_ADDR_LOOKUP);
        String wsDomainSubgroup = System.getProperty("rocketmq.namesrv.domain.subgroup", "nsaddr");
        String wsAddr = "http://" + wsDomainName + ":8080/rocketmq/" + wsDomainSubgroup;
        if (wsDomainName.indexOf(":") > 0) {
            wsAddr = "http://" + wsDomainName + "/rocketmq/" + wsDomainSubgroup;
        }
        return wsAddr;
    }

    public static String getRetryTopic(final String consumerGroup) {
        return RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
    }

    public static boolean isSysConsumerGroup(final String consumerGroup) {
        return consumerGroup.startsWith(CID_RMQ_SYS_PREFIX);
    }

    public static boolean isSystemTopic(final String topic) {
        return topic.startsWith(SYSTEM_TOPIC_PREFIX);
    }

    public static String getDLQTopic(final String consumerGroup) {
        return DLQ_GROUP_TOPIC_PREFIX + consumerGroup;
    }

    /**
     * broker的VIP通道
     *
     * @param isChange 是否改变
     * @param brokerAddr broker地址
     */
    public static String brokerVIPChannel(final boolean isChange, final String brokerAddr) {
        if (isChange) {
            //ip、端口号
            String[] ipAndPort = brokerAddr.split(":");
            //端口号减2，构建新的broker地址
            String brokerAddrNew = ipAndPort[0] + ":" + (Integer.parseInt(ipAndPort[1]) - 2);
            //返回新的broker地址
            return brokerAddrNew;
        } else {
            return brokerAddr;
        }
    }

    public static long getPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        if (processName != null && processName.length() > 0) {
            try {
                return Long.parseLong(processName.split("@")[0]);
            } catch (Exception e) {
                return 0;
            }
        }

        return 0;
    }

    /**
     * 将字符串存储到文件中
     *
     * @param str 要存储的内容
     * @param fileName 文件名
     */
    public static void string2File(final String str, final String fileName) throws IOException {

        String tmpFile = fileName + ".tmp";
        string2FileNotSafe(str, tmpFile);

        String bakFile = fileName + ".bak";
        //将以前的文件内容进行备份
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            //如果以前的文件内容不为空，对其内容进行备份
            string2FileNotSafe(prevContent, bakFile);
        }

        //删除以前的文件
        File file = new File(fileName);
        file.delete();

        //将临时文件名称修改成fileName
        file = new File(tmpFile);
        file.renameTo(new File(fileName));
    }

    /**
     * 将字符串存储到文件中，不是线程安全的
     *
     * @param str 要存储的内容
     * @param fileName 文件名
     */
    public static void string2FileNotSafe(final String str, final String fileName) throws IOException {
        File file = new File(fileName);
        //获取父类目
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            //父类目不存在，则创建
            fileParent.mkdirs();
        }
        FileWriter fileWriter = null;

        try {
            fileWriter = new FileWriter(file);
            //将要存储内容存储到文件中
            fileWriter.write(str);
        } catch (IOException e) {
            throw e;
        } finally {
            if (fileWriter != null) {
                //关闭文件流
                fileWriter.close();
            }
        }
    }

    /**
     * 根据文件名称读取文件内容
     *
     * @param fileName 文件名
     * @return 文件内容以字符串形式返回
     */
    public static String file2String(final String fileName) throws IOException {
        File file = new File(fileName);
        return file2String(file);
    }

    /**
     * 根据file读取文件内容
     *
     * @param file file文件
     * @return 文件内容以字符串形式返回
     */
    public static String file2String(final File file) throws IOException {
        if (file.exists()) {
            //创建文件内容长度的字节数组
            byte[] data = new byte[(int) file.length()];
            boolean result;

            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(file);
                //读取文件内容
                int len = inputStream.read(data);
                //文件内容是否等于预期的值
                result = len == data.length;
            } finally {
                if (inputStream != null) {
                    //关闭流
                    inputStream.close();
                }
            }

            if (result) {
                //返回文件内容
                return new String(data);
            }
        }
        return null;
    }

    public static String file2String(final URL url) {
        InputStream in = null;
        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.setUseCaches(false);
            in = urlConnection.getInputStream();
            int len = in.available();
            byte[] data = new byte[len];
            in.read(data, 0, len);
            return new String(data, "UTF-8");
        } catch (Exception ignored) {
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException ignored) {
                }
            }
        }

        return null;
    }

    /**
     * 打印对象的所有属性
     *
     * @param logger 日志
     * @param object 对象
     */
    public static void printObjectProperties(final InternalLogger logger, final Object object) {
        printObjectProperties(logger, object, false);
    }

    /**
     * 打印对象的属性
     *
     * @param logger 日志
     * @param object 对象
     * @param onlyImportantField {@code true}只打印对象的重要属性
     */
    public static void printObjectProperties(final InternalLogger logger, final Object object,
        final boolean onlyImportantField) {
        //获取所有属性
        Field[] fields = object.getClass().getDeclaredFields();
        //遍历所有属性
        for (Field field : fields) {
            //判断是否静态属性，如果是静态属性直接跳过
            if (!Modifier.isStatic(field.getModifiers())) {
                //获取属性名
                String name = field.getName();
                //如果属性名是this直接跳过
                if (!name.startsWith("this")) {
                    Object value = null;
                    try {
                        //反射对象禁止Java语言访问检查，这样才可以获取属性值
                        field.setAccessible(true);
                        //获取属性值
                        value = field.get(object);
                        if (null == value) {
                            //属性值为null，设置为空串
                            value = "";
                        }
                    } catch (IllegalAccessException e) {
                        //出现异常打印日志
                        log.error("Failed to obtain object properties", e);
                    }

                    //如果是只打印对象的重要属性，并且不是重要的属性，即没有ImportantField注解，直接跳过
                    if (onlyImportantField) {
                        Annotation annotation = field.getAnnotation(ImportantField.class);
                        if (null == annotation) {
                            continue;
                        }
                    }

                    if (logger != null) {
                        //如果日志记录器不为空，打印属性名和属性值
                        logger.info(name + "=" + value);
                    } else {
                    }
                }
            }
        }
    }

    public static String properties2String(final Properties properties) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getValue() != null) {
                sb.append(entry.getKey().toString() + "=" + entry.getValue().toString() + "\n");
            }
        }
        return sb.toString();
    }

    public static Properties string2Properties(final String str) {
        Properties properties = new Properties();
        try {
            InputStream in = new ByteArrayInputStream(str.getBytes(DEFAULT_CHARSET));
            properties.load(in);
        } catch (Exception e) {
            log.error("Failed to handle properties", e);
            return null;
        }

        return properties;
    }

    public static Properties object2Properties(final Object object) {
        Properties properties = new Properties();

        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                if (!name.startsWith("this")) {
                    Object value = null;
                    try {
                        field.setAccessible(true);
                        value = field.get(object);
                    } catch (IllegalAccessException e) {
                        log.error("Failed to handle properties", e);
                    }

                    if (value != null) {
                        properties.setProperty(name, value.toString());
                    }
                }
            }
        }

        return properties;
    }

    /**
     * 将属性列表转成对象
     *
     * @param p 属性列表
     * @param object 对象
     */
    public static void properties2Object(final Properties p, final Object object) {
        //获取对象的所有方法
        Method[] methods = object.getClass().getMethods();
        //遍历的所有方法
        for (Method method : methods) {
            //获取方法名
            String mn = method.getName();
            //过滤含有set方法
            if (mn.startsWith("set")) {
                try {
                    //获取属性名，除了第一个字符
                    String tmp = mn.substring(4);
                    //获取属性名的第一个字符
                    String first = mn.substring(3, 4);

                    //将属性名第一个字符转成小写加上其余字符
                    String key = first.toLowerCase() + tmp;
                    //获取属性值
                    String property = p.getProperty(key);
                    if (property != null) {
                        //获取方法的参数类型
                        Class<?>[] pt = method.getParameterTypes();
                        //方法存在参数
                        if (pt != null && pt.length > 0) {
                            //获取第一个参数的简称
                            String cn = pt[0].getSimpleName();
                            Object arg = null;
                            if (cn.equals("int") || cn.equals("Integer")) {
                                //参数类型是int或者Integer
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long") || cn.equals("Long")) {
                                //参数类型是long或者Long
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double") || cn.equals("Double")) {
                                //参数类型是double或者Double
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean") || cn.equals("Boolean")) {
                                //参数类型是boolean或者Boolean
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("float") || cn.equals("Float")) {
                                //参数类型是float或者Float
                                arg = Float.parseFloat(property);
                            } else if (cn.equals("String")) {
                                //参数类型是String
                                arg = property;
                            } else {
                                continue;
                            }
                            //反射调用设置值
                            method.invoke(object, arg);
                        }
                    }
                } catch (Throwable ignored) {
                    //出现异常，忽略
                }
            }
        }
    }

    public static boolean isPropertiesEqual(final Properties p1, final Properties p2) {
        return p1.equals(p2);
    }

    public static List<String> getLocalInetAddress() {
        List<String> inetAddressList = new ArrayList<String>();
        try {
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            while (enumeration.hasMoreElements()) {
                NetworkInterface networkInterface = enumeration.nextElement();
                Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    inetAddressList.add(addrs.nextElement().getHostAddress());
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException("get local inet address fail", e);
        }

        return inetAddressList;
    }

    private static String localhost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Throwable e) {
            try {
                String candidatesHost = getLocalhostByNetworkInterface();
                if (candidatesHost != null)
                    return candidatesHost;

            } catch (Exception ignored) {
            }

            throw new RuntimeException("InetAddress java.net.InetAddress.getLocalHost() throws UnknownHostException" + FAQUrl.suggestTodo(FAQUrl.UNKNOWN_HOST_EXCEPTION), e);
        }
    }

    //Reverse logic comparing to RemotingUtil method, consider refactor in RocketMQ 5.0
    public static String getLocalhostByNetworkInterface() throws SocketException {
        List<String> candidatesHost = new ArrayList<String>();
        Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();

        while (enumeration.hasMoreElements()) {
            NetworkInterface networkInterface = enumeration.nextElement();
            // Workaround for docker0 bridge
            if ("docker0".equals(networkInterface.getName()) || !networkInterface.isUp()) {
                continue;
            }
            Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
            while (addrs.hasMoreElements()) {
                InetAddress address = addrs.nextElement();
                if (address.isLoopbackAddress()) {
                    continue;
                }
                //ip4 highter priority
                if (address instanceof Inet6Address) {
                    candidatesHost.add(address.getHostAddress());
                    continue;
                }
                return address.getHostAddress();
            }
        }

        if (!candidatesHost.isEmpty()) {
            return candidatesHost.get(0);
        }
        return null;
    }

    public static boolean compareAndIncreaseOnly(final AtomicLong target, final long value) {
        long prev = target.get();
        while (value > prev) {
            boolean updated = target.compareAndSet(prev, value);
            if (updated)
                return true;

            prev = target.get();
        }

        return false;
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit)
            return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

}
