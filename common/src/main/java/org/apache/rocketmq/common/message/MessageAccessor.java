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

import java.util.Map;

/**
 * 消息的访问者
 * <p>
 *     消息的访问者模式
 * </p>
 */
public class MessageAccessor {

    /**
     * 清空消息的特定属性
     *
     * @param name 属性
     * @param msg 消息
     */
    public static void clearProperty(final Message msg, final String name) {
        msg.clearProperty(name);
    }

    /**
     * 设置消息的属性列表
     *
     * @param msg 消息
     * @param properties 属性列表
     */
    public static void setProperties(final Message msg, Map<String, String> properties) {
        msg.setProperties(properties);
    }

    /**
     * 设置消息的事务标识属性
     *
     * @param msg 消息
     * @param unit 事务标识
     */
    public static void setTransferFlag(final Message msg, String unit) {
        putProperty(msg, MessageConst.PROPERTY_TRANSFER_FLAG, unit);
    }

    /**
     * 将特定属性和属性值设置到属性列表中
     *
     * @param msg 消息
     * @param name 属性名
     * @param value 属性值
     */
    public static void putProperty(final Message msg, final String name, final String value) {
        msg.putProperty(name, value);
    }

    /**
     * 获取消息的事务标识
     *
     * @param msg 消息的事务标识
     * @return 消息的事务标识
     */
    public static String getTransferFlag(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_TRANSFER_FLAG);
    }

    /**
     * 设置消息的修正标识
     *
     * @param msg 消息
     * @param unit 修正标识
     */
    public static void setCorrectionFlag(final Message msg, String unit) {
        putProperty(msg, MessageConst.PROPERTY_CORRECTION_FLAG, unit);
    }

    /**
     * 获取消息的修正标识属性
     *
     * @param msg 消息
     */
    public static String getCorrectionFlag(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_CORRECTION_FLAG);
    }

    /**
     * 设置原消息id属性
     *
     * @param msg 消息
     * @param originMessageId 原消息id
     */
    public static void setOriginMessageId(final Message msg, String originMessageId) {
        putProperty(msg, MessageConst.PROPERTY_ORIGIN_MESSAGE_ID, originMessageId);
    }

    /**
     * 获取原消息id属性
     *
     * @param msg 消息
     * @return 原消息id
     */
    public static String getOriginMessageId(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
    }

    /**
     * 设置标识
     *
     * @param msg 消息
     * @param flag 标识
     */
    public static void setMQ2Flag(final Message msg, String flag) {
        putProperty(msg, MessageConst.PROPERTY_MQ2_FLAG, flag);
    }

    /**
     * 获取标识
     *
     * @param msg 消息
     * @消息标识
     */
    public static String getMQ2Flag(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_MQ2_FLAG);
    }

    /**
     * 设置消息的重试次数
     *
     * @param msg 消息
     * @param reconsumeTimes 重试次数
     */
    public static void setReconsumeTime(final Message msg, String reconsumeTimes) {
        putProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME, reconsumeTimes);
    }

    /**
     * 获取消息的重试次数
     *
     * @param msg 消息
     * @return 消息的重试次数
     */
    public static String getReconsumeTime(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_RECONSUME_TIME);
    }

    /**
     * 设置消息的最大重试次数
     *
     * @param msg 消息
     * @param maxReconsumeTimes 最大重试次数
     */
    public static void setMaxReconsumeTimes(final Message msg, String maxReconsumeTimes) {
        putProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES, maxReconsumeTimes);
    }

    /**
     * 获取消息的最大重试次数
     *
     * @param msg 消息
     * @return 消息的最大重试次数
     */
    public static String getMaxReconsumeTimes(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
    }

    /**
     * 设置消息开始消费的时间戳
     *
     * @param msg 消息
     * @param propertyConsumeStartTimeStamp 消息开始消费的时间戳
     */
    public static void setConsumeStartTimeStamp(final Message msg, String propertyConsumeStartTimeStamp) {
        putProperty(msg, MessageConst.PROPERTY_CONSUME_START_TIMESTAMP, propertyConsumeStartTimeStamp);
    }

    /**
     * 获取消息开始消费的时间戳
     *
     * @param msg 消息
     * @return 消息开始消费的时间戳
     */
    public static String getConsumeStartTimeStamp(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_CONSUME_START_TIMESTAMP);
    }

    /**
     * 克隆消息
     *
     * @param msg 消息
     * @return 克隆消息
     */
    public static Message cloneMessage(final Message msg) {
        Message newMsg = new Message(msg.getTopic(), msg.getBody());
        newMsg.setFlag(msg.getFlag());
        newMsg.setProperties(msg.getProperties());
        return newMsg;
    }

}
