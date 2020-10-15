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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消息解码实体
 */
public class MessageDecoder {
    /**
     * 消息id长度
     */
    public final static int MSG_ID_LENGTH = 8 + 8;

    /**
     * UTF-8字符集
     */
    public final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    /**
     * 消息魔数的位置
     */
    public final static int MESSAGE_MAGIC_CODE_POSTION = 4;
    /**
     * 消息标识位置
     */
    public final static int MESSAGE_FLAG_POSTION = 16;
    /**
     * 消息物理偏移位置
     */
    public final static int MESSAGE_PHYSIC_OFFSET_POSTION = 28;
    /**
     * 消息存储时间戳位置
     */
    public final static int MESSAGE_STORE_TIMESTAMP_POSTION = 56;
    /**
     * 消息魔数
     */
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    /**
     * name、值分割符
     */
    public static final char NAME_VALUE_SEPARATOR = 1;
    /**
     * 属性分割符
     */
    public static final char PROPERTY_SEPARATOR = 2;
    /**
     * 物理位置
     */
    public static final int PHY_POS_POSITION =  4 + 4 + 4 + 4 + 4 + 8;
    /**
     * 内容大小位置
     */
    public static final int BODY_SIZE_POSITION = 4 // 1 TOTALSIZE  总size
        + 4 // 2 MAGICCODE  魔数
        + 4 // 3 BODYCRC    内容crc
        + 4 // 4 QUEUEID    队列id
        + 4 // 5 FLAG       标识
        + 8 // 6 QUEUEOFFSET  队列偏移量
        + 8 // 7 PHYSICALOFFSET  物理偏移量
        + 4 // 8 SYSFLAG        系统标识
        + 8 // 9 BORNTIMESTAMP  消息产生的时间戳
        + 8 // 10 BORNHOST      消息产生的主机
        + 8 // 11 STORETIMESTAMP 消息存储的时间戳
        + 8 // 12 STOREHOSTADDRESS 消息存储主机
        + 4 // 13 RECONSUMETIMES  消息的重试次数
        + 8; // 14 Prepared Transaction Offset

    /**
     * 创建消息id
     *
     * @param input 输入缓冲区
     * @param addr 地址缓冲区
     * @param offset 偏移量
     */
    public static String createMessageId(final ByteBuffer input, final ByteBuffer addr, final long offset) {
        //翻转缓冲区
        input.flip();
        //设置限制
        input.limit(MessageDecoder.MSG_ID_LENGTH);
        //设置地址缓冲区
        input.put(addr);
        //设置偏移量
        input.putLong(offset);
        //将byte数组转成字符串
        return UtilAll.bytes2string(input.array());
    }

    /**
     * 创建消息id
     *
     * @param socketAddress 地址
     * @param transactionIdhashCode 事务id哈希code值
     * @return 消息id
     */
    public static String createMessageId(SocketAddress socketAddress, long transactionIdhashCode) {
        //分配16字节ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        //设置4个字节ip地址
        byteBuffer.put(inetSocketAddress.getAddress().getAddress());
        //设置4个字节端口号
        byteBuffer.putInt(inetSocketAddress.getPort());
        //设置8个字节事务id哈希code值
        byteBuffer.putLong(transactionIdhashCode);
        //翻转byteBuffer
        byteBuffer.flip();
        //将字节数组转成字符串
        return UtilAll.bytes2string(byteBuffer.array());
    }

    /**
     * 解码消息id
     *
     * @param msgId 消息id
     * @return 消息id实体
     */
    public static MessageId decodeMessageId(final String msgId) throws UnknownHostException {
        SocketAddress address;
        long offset;
        //消息id中的前8，是ip地址
        byte[] ip = UtilAll.string2bytes(msgId.substring(0, 8));
        //消息id中的8到16，是端口号
        byte[] port = UtilAll.string2bytes(msgId.substring(8, 16));
        //创建一个端口号的ByteBuffer
        ByteBuffer bb = ByteBuffer.wrap(port);
        //string2bytes长度为减半，为4个字节，获取4个字节的端口号
        int portInt = bb.getInt(0);
        //根据ip和端口号创建InetSocketAddress
        address = new InetSocketAddress(InetAddress.getByAddress(ip), portInt);

        // offset
        byte[] data = UtilAll.string2bytes(msgId.substring(16, 32));
        bb = ByteBuffer.wrap(data);
        //8字节的偏移量
        offset = bb.getLong(0);

        return new MessageId(address, offset);
    }

    /**
     * 只解码msg缓冲区的属性
     *
     * @param byteBuffer 消息提交日志缓冲区
     */
    public static Map<String, String> decodeProperties(java.nio.ByteBuffer byteBuffer) {
        int topicLengthPosition = BODY_SIZE_POSITION + 4 + byteBuffer.getInt(BODY_SIZE_POSITION);

        byte topicLength = byteBuffer.get(topicLengthPosition);

        //2个字节的属性长度，propertiesLengthPosition = topicLengthPosition + topicLength + topic内容
        short propertiesLength = byteBuffer.getShort(topicLengthPosition + 1 + topicLength);

        //属性内容开始位置
        byteBuffer.position(topicLengthPosition + 1 + topicLength + 2);

        if (propertiesLength > 0) {
            byte[] properties = new byte[propertiesLength];
            byteBuffer.get(properties);
            //属性内容
            String propertiesString = new String(properties, CHARSET_UTF8);
            //将属性内容字符串转成map
            Map<String, String> map = string2messageProperties(propertiesString);
            return map;
        }
        return null;
    }

    /**
     * 对消息缓冲区进行解码
     *
     * @param byteBuffer 字节缓冲区
     * @return 扩展的消息体
     */
    public static MessageExt decode(java.nio.ByteBuffer byteBuffer) {
        return decode(byteBuffer, true, true, false);
    }

    /**
     * 客户端解码
     *
     * @param byteBuffer 字节缓冲区
     * @param readBody 读消息内容
     * @return 扩展的消息体
     */
    public static MessageExt clientDecode(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        return decode(byteBuffer, readBody, true, true);
    }

    /**
     * 对消息缓冲区进行解码
     *
     * @param byteBuffer 字节缓冲区
     * @param readBody 读消息内容
     * @return 扩展的消息体
     */
    public static MessageExt decode(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        return decode(byteBuffer, readBody, true, false);
    }

    /**
     * 对扩展消息进行编码
     *
     * @param messageExt 扩展消息
     * @param needCompress 需要进行压缩
     * @return 编码后字节数组
     */
    public static byte[] encode(MessageExt messageExt, boolean needCompress) throws Exception {
        //获取到消息内容
        byte[] body = messageExt.getBody();
        //消息主题的字节数组
        byte[] topics = messageExt.getTopic().getBytes(CHARSET_UTF8);
        //消息主题的字节数组长度
        byte topicLen = (byte) topics.length;
        //消息属性map转成字符串
        String properties = messageProperties2String(messageExt.getProperties());
        //将属性字符串转成字节数组
        byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
        //字节数组长度
        short propertiesLength = (short) propertiesBytes.length;
        //系统标识
        int sysFlag = messageExt.getSysFlag();
        byte[] newBody = messageExt.getBody();
        if (needCompress && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
            //对消息内容进行压缩
            newBody = UtilAll.compress(body, 5);
        }
        //内容长度
        int bodyLength = newBody.length;
        //消息的存储大小
        int storeSize = messageExt.getStoreSize();
        ByteBuffer byteBuffer;
        if (storeSize > 0) {
            byteBuffer = ByteBuffer.allocate(storeSize);
        } else {
            storeSize = 4 // 1 TOTALSIZE
                + 4 // 2 MAGICCODE
                + 4 // 3 BODYCRC
                + 4 // 4 QUEUEID
                + 4 // 5 FLAG
                + 8 // 6 QUEUEOFFSET
                + 8 // 7 PHYSICALOFFSET
                + 4 // 8 SYSFLAG
                + 8 // 9 BORNTIMESTAMP
                + 8 // 10 BORNHOST
                + 8 // 11 STORETIMESTAMP
                + 8 // 12 STOREHOSTADDRESS
                + 4 // 13 RECONSUMETIMES
                + 8 // 14 Prepared Transaction Offset
                + 4 + bodyLength // 14 BODY
                + 1 + topicLen // 15 TOPIC
                + 2 + propertiesLength // 16 propertiesLength
                + 0;
            byteBuffer = ByteBuffer.allocate(storeSize);
        }
        // 1 TOTALSIZE
        byteBuffer.putInt(storeSize);

        // 2 MAGICCODE
        byteBuffer.putInt(MESSAGE_MAGIC_CODE);

        // 3 BODYCRC
        int bodyCRC = messageExt.getBodyCRC();
        byteBuffer.putInt(bodyCRC);

        // 4 QUEUEID
        int queueId = messageExt.getQueueId();
        byteBuffer.putInt(queueId);

        // 5 FLAG
        int flag = messageExt.getFlag();
        byteBuffer.putInt(flag);

        // 6 QUEUEOFFSET
        long queueOffset = messageExt.getQueueOffset();
        byteBuffer.putLong(queueOffset);

        // 7 PHYSICALOFFSET
        long physicOffset = messageExt.getCommitLogOffset();
        byteBuffer.putLong(physicOffset);

        // 8 SYSFLAG
        byteBuffer.putInt(sysFlag);

        // 9 BORNTIMESTAMP
        long bornTimeStamp = messageExt.getBornTimestamp();
        byteBuffer.putLong(bornTimeStamp);

        // 10 BORNHOST
        InetSocketAddress bornHost = (InetSocketAddress) messageExt.getBornHost();
        byteBuffer.put(bornHost.getAddress().getAddress());
        byteBuffer.putInt(bornHost.getPort());

        // 11 STORETIMESTAMP
        long storeTimestamp = messageExt.getStoreTimestamp();
        byteBuffer.putLong(storeTimestamp);

        // 12 STOREHOST
        InetSocketAddress serverHost = (InetSocketAddress) messageExt.getStoreHost();
        byteBuffer.put(serverHost.getAddress().getAddress());
        byteBuffer.putInt(serverHost.getPort());

        // 13 RECONSUMETIMES
        int reconsumeTimes = messageExt.getReconsumeTimes();
        byteBuffer.putInt(reconsumeTimes);

        // 14 Prepared Transaction Offset
        long preparedTransactionOffset = messageExt.getPreparedTransactionOffset();
        byteBuffer.putLong(preparedTransactionOffset);

        // 15 BODY
        byteBuffer.putInt(bodyLength);
        byteBuffer.put(newBody);

        // 16 TOPIC
        byteBuffer.put(topicLen);
        byteBuffer.put(topics);

        // 17 properties
        byteBuffer.putShort(propertiesLength);
        byteBuffer.put(propertiesBytes);

        return byteBuffer.array();
    }

    /**
     * 对消息缓冲区进行解码
     *
     * @param byteBuffer 字节缓冲区
     * @param readBody 是否读消息内容
     * @param deCompressBody 是否对消息体进行解压
     * @return 扩展的消息体
     */
    public static MessageExt decode(
        java.nio.ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody) {
        return decode(byteBuffer, readBody, deCompressBody, false);
    }

    /**
     * 对消息缓冲区进行解码
     *
     * @param byteBuffer 字节缓冲区
     * @param readBody 是否读取内容
     * @param deCompressBody 是否解压缩内容
     * @param isClient 是否客户端
     * @return 扩展的消息体
     */
    public static MessageExt decode(
        java.nio.ByteBuffer byteBuffer, final boolean readBody, final boolean deCompressBody, final boolean isClient) {
        try {

            MessageExt msgExt;
            if (isClient) {
                msgExt = new MessageClientExt();
            } else {
                msgExt = new MessageExt();
            }

            // 1 TOTALSIZE  4个字节的总长度
            int storeSize = byteBuffer.getInt();
            //设置总长度
            msgExt.setStoreSize(storeSize);

            // 2 MAGICCODE，4个字节的魔数
            byteBuffer.getInt();

            // 3 BODYCRC，4个字节的内容crc
            int bodyCRC = byteBuffer.getInt();
            //设置内容crc
            msgExt.setBodyCRC(bodyCRC);

            // 4 QUEUEID，4个字节的队列id
            int queueId = byteBuffer.getInt();
            msgExt.setQueueId(queueId);

            // 5 FLAG
            int flag = byteBuffer.getInt();
            //设置消息标识
            msgExt.setFlag(flag);

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();
            //设置队列偏移量
            msgExt.setQueueOffset(queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();
            //设置物理偏移量
            msgExt.setCommitLogOffset(physicOffset);

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();
            //设置系统标识
            msgExt.setSysFlag(sysFlag);

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            //设置消息诞生的时间
            msgExt.setBornTimestamp(bornTimeStamp);

            // 10 BORNHOST
            byte[] bornHost = new byte[4];
            //4个字节的诞生ip
            byteBuffer.get(bornHost, 0, 4);
            //端口号
            int port = byteBuffer.getInt();
            //设置诞生的主机
            msgExt.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();
            //设置消息的存储时间
            msgExt.setStoreTimestamp(storeTimestamp);

            // 12 STOREHOST
            byte[] storeHost = new byte[4];
            //4个字节的存储ip地址
            byteBuffer.get(storeHost, 0, 4);
            //4个字节的端口号
            port = byteBuffer.getInt();
            msgExt.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHost), port));

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuffer.getInt();
            //设置重试次数
            msgExt.setReconsumeTimes(reconsumeTimes);

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();
            //设置事务的半消息偏移量
            msgExt.setPreparedTransactionOffset(preparedTransactionOffset);

            // 15 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                //内容长度不为空，并且readBody为true
                if (readBody) {
                    byte[] body = new byte[bodyLen];
                    //设置内容
                    byteBuffer.get(body);

                    // uncompress body
                    if (deCompressBody && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
                        //解压后的字节数组
                        body = UtilAll.uncompress(body);
                    }
                    //设置内容
                    msgExt.setBody(body);
                } else {
                    //否则跳过当前内容
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 16 TOPIC，1个字节的topic长度
            byte topicLen = byteBuffer.get();
            byte[] topic = new byte[(int) topicLen];
            //获取topic内容
            byteBuffer.get(topic);
            //设置topic
            msgExt.setTopic(new String(topic, CHARSET_UTF8));

            // 17 properties，2个字节的属性长度
            short propertiesLength = byteBuffer.getShort();
            if (propertiesLength > 0) {
                byte[] properties = new byte[propertiesLength];
                //获取属性内容
                byteBuffer.get(properties);
                String propertiesString = new String(properties, CHARSET_UTF8);
                //将属性字符串转成map
                Map<String, String> map = string2messageProperties(propertiesString);
                //设置消息属性
                msgExt.setProperties(map);
            }

            ByteBuffer byteBufferMsgId = ByteBuffer.allocate(MSG_ID_LENGTH);
            //创建消息的id
            String msgId = createMessageId(byteBufferMsgId, msgExt.getStoreHostBytes(), msgExt.getCommitLogOffset());
            //设置消息id
            msgExt.setMsgId(msgId);

            if (isClient) {
                //如果是客户端，
                ((MessageClientExt) msgExt).setOffsetMsgId(msgId);
            }

            return msgExt;
        } catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return null;
    }

    /**
     * 批量对消息缓冲区进行解码
     *
     * @param byteBuffer 字节缓冲区
     * @return 扩展的消息体列表
     */
    public static List<MessageExt> decodes(java.nio.ByteBuffer byteBuffer) {
        return decodes(byteBuffer, true);
    }

    /**
     * 批量对消息缓冲区进行解码
     *
     * @param byteBuffer 字节缓冲区
     * @param readBody 读消息内容
     * @return 扩展的消息体列表
     */
    public static List<MessageExt> decodes(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        List<MessageExt> msgExts = new ArrayList<MessageExt>();
        while (byteBuffer.hasRemaining()) {
            MessageExt msgExt = clientDecode(byteBuffer, readBody);
            if (null != msgExt) {
                msgExts.add(msgExt);
            } else {
                break;
            }
        }
        return msgExts;
    }

    /**
     * 将属性内容map转成字符串
     *
     * @param properties 属性map
     * @return 属性字符串
     */
    public static String messageProperties2String(Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        if (properties != null) {
            for (final Map.Entry<String, String> entry : properties.entrySet()) {
                //属性key
                final String name = entry.getKey();
                //属性值
                final String value = entry.getValue();

                //使用NAME_VALUE_SEPARATOR分割name、value，使用PROPERTY_SEPARATOR分割每个属性
                sb.append(name);
                sb.append(NAME_VALUE_SEPARATOR);
                sb.append(value);
                sb.append(PROPERTY_SEPARATOR);
            }
        }
        return sb.toString();
    }

    /**
     * 将属性内容字符串转成map
     *
     * @param properties 属性字符串
     * @return 属性map
     */
    public static Map<String, String> string2messageProperties(final String properties) {
        Map<String, String> map = new HashMap<String, String>();
        if (properties != null) {
            //使用属性分隔符，将属性分开
            String[] items = properties.split(String.valueOf(PROPERTY_SEPARATOR));
            for (String i : items) {
                //使用name、value分割符
                String[] nv = i.split(String.valueOf(NAME_VALUE_SEPARATOR));
                if (2 == nv.length) {
                    map.put(nv[0], nv[1]);
                }
            }
        }

        return map;
    }

    /**
     * 对消息体进行编码
     *
     * @param message 消息体
     * @return 编码后的消息体字节码数组
     */
    public static byte[] encodeMessage(Message message) {
        //only need flag, body, properties
        //消息内容
        byte[] body = message.getBody();
        //消息长度
        int bodyLen = body.length;
        //将消息属性map转成字符串
        String properties = messageProperties2String(message.getProperties());
        //将属性字符串转成字节数组
        byte[] propertiesBytes = properties.getBytes(CHARSET_UTF8);
        //note properties length must not more than Short.MAX
        short propertiesLength = (short) propertiesBytes.length;
        int sysFlag = message.getFlag();
        int storeSize = 4 // 1 TOTALSIZE
            + 4 // 2 MAGICCOD
            + 4 // 3 BODYCRC
            + 4 // 4 FLAG
            + 4 + bodyLen // 4 BODY
            + 2 + propertiesLength;
        ByteBuffer byteBuffer = ByteBuffer.allocate(storeSize);
        // 1 TOTALSIZE
        byteBuffer.putInt(storeSize);

        // 2 MAGICCODE
        byteBuffer.putInt(0);

        // 3 BODYCRC
        byteBuffer.putInt(0);

        // 4 FLAG
        int flag = message.getFlag();
        byteBuffer.putInt(flag);

        // 5 BODY
        byteBuffer.putInt(bodyLen);
        byteBuffer.put(body);

        // 6 properties
        byteBuffer.putShort(propertiesLength);
        byteBuffer.put(propertiesBytes);

        return byteBuffer.array();
    }

    public static Message decodeMessage(ByteBuffer byteBuffer) throws Exception {
        Message message = new Message();

        // 1 TOTALSIZE
        byteBuffer.getInt();

        // 2 MAGICCODE
        byteBuffer.getInt();

        // 3 BODYCRC
        byteBuffer.getInt();

        // 4 FLAG
        int flag = byteBuffer.getInt();
        message.setFlag(flag);

        // 5 BODY
        int bodyLen = byteBuffer.getInt();
        byte[] body = new byte[bodyLen];
        byteBuffer.get(body);
        message.setBody(body);

        // 6 properties
        short propertiesLen = byteBuffer.getShort();
        byte[] propertiesBytes = new byte[propertiesLen];
        byteBuffer.get(propertiesBytes);
        message.setProperties(string2messageProperties(new String(propertiesBytes, CHARSET_UTF8)));

        return message;
    }

    public static byte[] encodeMessages(List<Message> messages) {
        //TO DO refactor, accumulate in one buffer, avoid copies
        List<byte[]> encodedMessages = new ArrayList<byte[]>(messages.size());
        int allSize = 0;
        for (Message message : messages) {
            byte[] tmp = encodeMessage(message);
            encodedMessages.add(tmp);
            allSize += tmp.length;
        }
        byte[] allBytes = new byte[allSize];
        int pos = 0;
        for (byte[] bytes : encodedMessages) {
            System.arraycopy(bytes, 0, allBytes, pos, bytes.length);
            pos += bytes.length;
        }
        return allBytes;
    }

    public static List<Message> decodeMessages(ByteBuffer byteBuffer) throws Exception {
        //TO DO add a callback for processing,  avoid creating lists
        List<Message> msgs = new ArrayList<Message>();
        while (byteBuffer.hasRemaining()) {
            Message msg = decodeMessage(byteBuffer);
            msgs.add(msg);
        }
        return msgs;
    }
}
