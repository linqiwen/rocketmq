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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class UtilAll {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd#HH:mm:ss:SSS";
    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
    final static char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static int getPid() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName(); // format: "pid@hostname"
        try {
            return Integer.parseInt(name.substring(0, name.indexOf('@')));
        } catch (Exception e) {
            return -1;
        }
    }

    public static void sleep(long sleepMs) {
        if (sleepMs < 0) {
            return;
        }
        try {
            Thread.sleep(sleepMs);
        } catch (Throwable ignored) {

        }

    }

    public static String currentStackTrace() {
        StringBuilder sb = new StringBuilder();
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement ste : stackTrace) {
            sb.append("\n\t");
            sb.append(ste.toString());
        }

        return sb.toString();
    }

    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static long computeEclipseTimeMilliseconds(final long beginTime) {
        return System.currentTimeMillis() - beginTime;
    }

    public static boolean isItTimeToDo(final String when) {
        String[] whiles = when.split(";");
        if (whiles.length > 0) {
            Calendar now = Calendar.getInstance();
            for (String w : whiles) {
                int nowHour = Integer.parseInt(w);
                if (nowHour == now.get(Calendar.HOUR_OF_DAY)) {
                    return true;
                }
            }
        }

        return false;
    }

    public static String timeMillisToHumanString() {
        return timeMillisToHumanString(System.currentTimeMillis());
    }

    public static String timeMillisToHumanString(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return String.format("%04d%02d%02d%02d%02d%02d%03d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
            cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),
            cal.get(Calendar.MILLISECOND));
    }

    public static long computNextMorningTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }

    public static long computNextMinutesTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 0);
        cal.add(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }

    public static long computNextHourTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }

    public static long computNextHalfHourTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 1);
        cal.set(Calendar.MINUTE, 30);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }

    public static String timeMillisToHumanString2(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d,%03d",
            cal.get(Calendar.YEAR),
            cal.get(Calendar.MONTH) + 1,
            cal.get(Calendar.DAY_OF_MONTH),
            cal.get(Calendar.HOUR_OF_DAY),
            cal.get(Calendar.MINUTE),
            cal.get(Calendar.SECOND),
            cal.get(Calendar.MILLISECOND));
    }

    public static String timeMillisToHumanString3(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return String.format("%04d%02d%02d%02d%02d%02d",
            cal.get(Calendar.YEAR),
            cal.get(Calendar.MONTH) + 1,
            cal.get(Calendar.DAY_OF_MONTH),
            cal.get(Calendar.HOUR_OF_DAY),
            cal.get(Calendar.MINUTE),
            cal.get(Calendar.SECOND));
    }

    public static double getDiskPartitionSpaceUsedPercent(final String path) {
        if (null == path || path.isEmpty())
            return -1;

        try {
            File file = new File(path);

            if (!file.exists())
                return -1;

            long totalSpace = file.getTotalSpace();

            if (totalSpace > 0) {
                long freeSpace = file.getFreeSpace();
                long usedSpace = totalSpace - freeSpace;

                return usedSpace / (double) totalSpace;
            }
        } catch (Exception e) {
            return -1;
        }

        return -1;
    }

    public static int crc32(byte[] array) {
        if (array != null) {
            return crc32(array, 0, array.length);
        }

        return 0;
    }

    public static int crc32(byte[] array, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(array, offset, length);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }

    /**
     * byte数组转成字符串
     *
     * @param src byte数组
     * @return 字符串
     */
    public static String bytes2string(byte[] src) {
        //十六进制字符串的char数组，长度byte数组长度的两倍
        char[] hexChars = new char[src.length * 2];
        //遍历byte数组
        for (int j = 0; j < src.length; j++) {
            //获取每个byte值
            int v = src[j] & 0xFF;
            //byte值的高四位j * 2位置的char值
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            //byte值的低四位j * 2 + 1位置的char值
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        //十六进制字符串的char数组转成字符串
        return new String(hexChars);
    }

    /**
     * 字符串转byte
     *
     * @param hexString 十六进制字符串
     * @return byte数组
     */
    public static byte[] string2bytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
            //十六进制字符串为null或者空串返回null
            return null;
        }
        //全部转成大写
        hexString = hexString.toUpperCase();
        //char数组长度的一半
        int length = hexString.length() / 2;
        //获取字符串的char数组
        char[] hexChars = hexString.toCharArray();
        //创建字节数组
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            //每次获取char数组中的两个字符
            int pos = i * 2;
            //设置每个byte数组
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }

    /**
     * char转成byte
     */
    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

    /**
     * 对内容字节数组进行解压
     *
     * @param src 待解压的字节数组
     * @return 解压后的字节数组
     */
    public static byte[] uncompress(final byte[] src) throws IOException {
        byte[] result = src;
        byte[] uncompressData = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

        try {
            while (true) {
                int len = inflaterInputStream.read(uncompressData, 0, uncompressData.length);
                if (len <= 0) {
                    break;
                }
                //将uncompressData写入到输出流中
                byteArrayOutputStream.write(uncompressData, 0, len);
            }
            byteArrayOutputStream.flush();
            //获取字节数组
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                //关闭字节数组输入流
                byteArrayInputStream.close();
            } catch (IOException e) {
                log.error("Failed to close the stream", e);
            }
            try {
                //关闭增压输入流
                inflaterInputStream.close();
            } catch (IOException e) {
                log.error("Failed to close the stream", e);
            }
            try {
                //关闭字节数组输出流
                byteArrayOutputStream.close();
            } catch (IOException e) {
                log.error("Failed to close the stream", e);
            }
        }

        return result;
    }

    /**
     * 对字节数组进行压缩
     *
     * @param src 字节数组
     * @param level 压缩等级
     * @return 压缩后的字节数组
     */
    public static byte[] compress(final byte[] src, final int level) throws IOException {
        byte[] result = src;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        java.util.zip.Deflater defeater = new java.util.zip.Deflater(level);
        //定义压缩输出流
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream, defeater);
        try {
            deflaterOutputStream.write(src);
            deflaterOutputStream.finish();
            deflaterOutputStream.close();
            //获取压缩后的字节流
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            //关闭压缩器并丢弃任何未处理的输入。
            //当压缩器不再工作时，应调用此方法
            //正在使用，但也将由finalize（）方法自动调用。调用此方法后，Deflater对象的行为将是未定义的
            defeater.end();
            throw e;
        } finally {
            try {
                //关闭输出流
                byteArrayOutputStream.close();
            } catch (IOException ignored) {
            }

            defeater.end();
        }

        return result;
    }

    public static int asInt(String str, int defaultValue) {
        try {
            return Integer.parseInt(str);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static long asLong(String str, long defaultValue) {
        try {
            return Long.parseLong(str);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static String formatDate(Date date, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        return df.format(date);
    }

    /**
     * 解析时间
     *
     * @param date 时间字符串
     * @param pattern 格式
     */
    public static Date parseDate(String date, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        try {
            return df.parse(date);
        } catch (ParseException e) {
            return null;
        }
    }

    public static String responseCode2String(final int code) {
        return Integer.toString(code);
    }

    public static String frontStringAtLeast(final String str, final int size) {
        if (str != null) {
            if (str.length() > size) {
                return str.substring(0, size);
            }
        }

        return str;
    }

    /**
     * 判断字符串是否为null或者空串
     *
     * @param str 要检验的字符串
     * @return {@code true} 字符串为null或者空串
     */
    public static boolean isBlank(String str) {
        int strLen;
        //字符串为null或者空串
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        //遍历字符串中的字符
        for (int i = 0; i < strLen; i++) {
            //如果存在
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static String jstack() {
        return jstack(Thread.getAllStackTraces());
    }

    public static String jstack(Map<Thread, StackTraceElement[]> map) {
        StringBuilder result = new StringBuilder();
        try {
            Iterator<Map.Entry<Thread, StackTraceElement[]>> ite = map.entrySet().iterator();
            while (ite.hasNext()) {
                Map.Entry<Thread, StackTraceElement[]> entry = ite.next();
                StackTraceElement[] elements = entry.getValue();
                Thread thread = entry.getKey();
                if (elements != null && elements.length > 0) {
                    String threadName = entry.getKey().getName();
                    result.append(String.format("%-40sTID: %d STATE: %s%n", threadName, thread.getId(), thread.getState()));
                    for (StackTraceElement el : elements) {
                        result.append(String.format("%-40s%s%n", threadName, el.toString()));
                    }
                    result.append("\n");
                }
            }
        } catch (Throwable e) {
            result.append(RemotingHelper.exceptionSimpleDesc(e));
        }

        return result.toString();
    }

    public static boolean isInternalIP(byte[] ip) {
        if (ip.length != 4) {
            throw new RuntimeException("illegal ipv4 bytes");
        }

        //10.0.0.0~10.255.255.255
        //172.16.0.0~172.31.255.255
        //192.168.0.0~192.168.255.255
        if (ip[0] == (byte) 10) {

            return true;
        } else if (ip[0] == (byte) 172) {
            if (ip[1] >= (byte) 16 && ip[1] <= (byte) 31) {
                return true;
            }
        } else if (ip[0] == (byte) 192) {
            if (ip[1] == (byte) 168) {
                return true;
            }
        }
        return false;
    }

    private static boolean ipCheck(byte[] ip) {
        if (ip.length != 4) {
            throw new RuntimeException("illegal ipv4 bytes");
        }

//        if (ip[0] == (byte)30 && ip[1] == (byte)10 && ip[2] == (byte)163 && ip[3] == (byte)120) {
//        }

        //A类地址，第1字节为网络地址，其它3个字节为主机地址
        if (ip[0] >= (byte) 1 && ip[0] <= (byte) 126) {
            if (ip[1] == (byte) 1 && ip[2] == (byte) 1 && ip[3] == (byte) 1) {
                return false;
            }
            //预留地址
            if (ip[1] == (byte) 0 && ip[2] == (byte) 0 && ip[3] == (byte) 0) {
                return false;
            }
            return true;
        //B类地址，第1字节、第2字节为网络地址，最后2个字节为主机地址
        } else if (ip[0] >= (byte) 128 && ip[0] <= (byte) 191) {
            if (ip[2] == (byte) 1 && ip[3] == (byte) 1) {
                return false;
            }
            if (ip[2] == (byte) 0 && ip[3] == (byte) 0) {
                return false;
            }
            return true;
        //C类地址，第1字节、第2字节、第3字节网络地址，最后1个字节为主机地址
        } else if (ip[0] >= (byte) 192 && ip[0] <= (byte) 223) {
            if (ip[3] == (byte) 1) {
                return false;
            }
            if (ip[3] == (byte) 0) {
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * 将字节数组转成ip字符串
     *
     * @param ip ipv4字节数组
     * @return 字符串ipv4
     */
    public static String ipToIPv4Str(byte[] ip) {
        //非法的字节数组，直接返回null
        if (ip.length != 4) {
            return null;
        }
        return new StringBuilder().append(ip[0] & 0xFF).append(".").append(
            ip[1] & 0xFF).append(".").append(ip[2] & 0xFF)
            .append(".").append(ip[3] & 0xFF).toString();
    }

    public static byte[] getIP() {
        try {
            Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            byte[] internalIP = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                Enumeration addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = (InetAddress) addresses.nextElement();
                    if (ip != null && ip instanceof Inet4Address) {
                        byte[] ipByte = ip.getAddress();
                        if (ipByte.length == 4) {
                            if (ipCheck(ipByte)) {
                                if (!isInternalIP(ipByte)) {
                                    return ipByte;
                                } else if (internalIP == null) {
                                    internalIP = ipByte;
                                }
                            }
                        }
                    }
                }
            }
            if (internalIP != null) {
                return internalIP;
            } else {
                throw new RuntimeException("Can not get local ip");
            }
        } catch (Exception e) {
            throw new RuntimeException("Can not get local ip", e);
        }
    }

    public static void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                deleteFile(file1);
            }
            file.delete();
        }
    }
}
