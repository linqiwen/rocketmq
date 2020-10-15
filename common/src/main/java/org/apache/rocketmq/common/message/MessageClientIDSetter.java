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
package org.apache.rocketmq.common.message;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.UtilAll;

/**
 * 消息客户端IDSetter
 */
public class MessageClientIDSetter {
    /**
     * 主题key的分隔符
     */
    private static final String TOPIC_KEY_SPLITTER = "#";
    private static final int LEN;
    /**
     * 固定字符串
     */
    private static final String FIX_STRING;
    /**
     * 计数器
     */
    private static final AtomicInteger COUNTER;
    /**
     * 开始时间
     */
    private static long startTime;
    /**
     * 下一个开始时间
     */
    private static long nextStartTime;

    static {
        LEN = 4 + 2 + 4 + 4 + 2;
        ByteBuffer tempBuffer = ByteBuffer.allocate(10);
        tempBuffer.position(2);
        tempBuffer.putInt(UtilAll.getPid());
        tempBuffer.position(0);
        try {
            tempBuffer.put(UtilAll.getIP());
        } catch (Exception e) {
            tempBuffer.put(createFakeIP());
        }
        tempBuffer.position(6);
        tempBuffer.putInt(MessageClientIDSetter.class.getClassLoader().hashCode());
        //固定字符串
        FIX_STRING = UtilAll.bytes2string(tempBuffer.array());
        setStartTime(System.currentTimeMillis());
        COUNTER = new AtomicInteger(0);
    }

    private synchronized static void setStartTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        //传入时间月份的第一天
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        startTime = cal.getTimeInMillis();
        //传入时间的下一个月
        cal.add(Calendar.MONTH, 1);
        nextStartTime = cal.getTimeInMillis();
    }

    public static Date getNearlyTimeFromID(String msgID) {
        //分配8个字节的ByteBuffer
        ByteBuffer buf = ByteBuffer.allocate(8);
        //将msgId字符串转成bytes数组
        byte[] bytes = UtilAll.string2bytes(msgID);
        //前四个字节都设置成0
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        //从byte数组的偏移量10位置获取4个字节
        buf.put(bytes, 10, 4);
        //将位置重新设置为0
        buf.position(0);
        //转成long值
        long spanMS = buf.getLong();
        Calendar cal = Calendar.getInstance();
        //获取当前时间
        long now = cal.getTimeInMillis();
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        long monStartTime = cal.getTimeInMillis();
        //当前月份的一天00:00:00:00加上spanMS是否大于等于当前时间
        if (monStartTime + spanMS >= now) {
            //如果是前一个月
            cal.add(Calendar.MONTH, -1);
            //重新获取前一个月时间
            monStartTime = cal.getTimeInMillis();
        }
        //重新设置时间
        cal.setTimeInMillis(monStartTime + spanMS);
        return cal.getTime();
    }

    /**
     * 从消息id中获取ipv4字符串
     */
    public static String getIPStrFromID(String msgID) {
        byte[] ipBytes = getIPFromID(msgID);
        //将字节数组转成ipv4字符串
        return UtilAll.ipToIPv4Str(ipBytes);
    }

    /**
     * 从消息id中获取前4个字节数组
     */
    public static byte[] getIPFromID(String msgID) {
        //4个长度的字节数组
        byte[] result = new byte[4];
        //字符串转byte数组
        byte[] bytes = UtilAll.string2bytes(msgID);
        //获取byte数组的前4个字节
        System.arraycopy(bytes, 0, result, 0, 4);
        return result;
    }

    /**
     * 创建唯一id
     *
     * @return 唯一id串
     */
    public static String createUniqID() {
        StringBuilder sb = new StringBuilder(LEN * 2);
        //添加固定字符串，FIX_STRING占20个字节，
        sb.append(FIX_STRING);
        //添加唯一id串，占12个字节
        sb.append(UtilAll.bytes2string(createUniqIDBuffer()));
        return sb.toString();
    }

    /**
     * 创建唯一id缓冲区
     */
    private static byte[] createUniqIDBuffer() {
        //创建6个字节的ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(4 + 2);
        //获取当前时间
        long current = System.currentTimeMillis();
        //当前时间大于下一个开始时间
        if (current >= nextStartTime) {
            //重新设置开始时间和下一个开始时间
            setStartTime(current);
        }
        buffer.position(0);
        //设置开始时间和当前时间的时间差
        buffer.putInt((int) (System.currentTimeMillis() - startTime));
        //次数
        buffer.putShort((short) COUNTER.getAndIncrement());
        return buffer.array();
    }

    /**
     * 消息的扩展属性中添加唯一id
     *
     * @param msg 消息
     */
    public static void setUniqID(final Message msg) {
        if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null) {
            msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
        }
    }

    /**
     * 获取消息的唯一id
     *
     * @param msg 消息
     */
    public static String getUniqID(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }

    /**
     * 创建假的ip
     */
    public static byte[] createFakeIP() {
        //分配8个字节的ByteBuffer
        ByteBuffer bb = ByteBuffer.allocate(8);
        //当前时间设置到bb中
        bb.putLong(System.currentTimeMillis());
        //位置设置为4
        bb.position(4);
        byte[] fakeIP = new byte[4];
        //从位置为4进行获取4个字节的ip信息
        bb.get(fakeIP);
        return fakeIP;
    }
}
    
