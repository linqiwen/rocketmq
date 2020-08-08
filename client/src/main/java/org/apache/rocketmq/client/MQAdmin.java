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
package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * MQ管理的基本接口
 */
public interface MQAdmin {
    /**
     * 创建一个主题
     *
     * @param key accesskey
     * @param newTopic 主题名称
     * @param queueNum 主题队列数量
     */
    void createTopic(final String key, final String newTopic, final int queueNum)
        throws MQClientException;

    /**
     * 创建一个主题
     *
     * @param key accesskey
     * @param newTopic 主题名称
     * @param queueNum 主题队列数量
     * @param topicSysFlag topic system flag
     */
    void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
        throws MQClientException;

    /**
     * 获取根据某个时间（以毫秒为单位）的消息队列偏移量
     * 调用要小心，因为IO开销过大
     *
     * @param mq MessageQueue实例
     * @param timestamp from when in milliseconds.
     * @return 消息偏移量
     */
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;

    /**
     * 获取最大偏移量
     *
     * @param mq MessageQueue实例
     * @return 最大偏移量
     */
    long maxOffset(final MessageQueue mq) throws MQClientException;

    /**
     * 获取最小偏移量
     *
     * @param mq MessageQueue实例
     * @return 最小偏移量
     */
    long minOffset(final MessageQueue mq) throws MQClientException;

    /**
     * 最早被存储消息的时间
     *
     * @param mq MessageQueue实例
     * @return 最早被存储消息的时间，时间以微秒为单位
     */
    long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;

    /**
     * 查询消息根据消息id
     *
     * @param offsetMsgId 消息id
     * @return message 消息
     */
    MessageExt viewMessage(final String offsetMsgId) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    /**
     * 查询消息
     *
     * @param topic 消息主题
     * @param key 消息键索引词
     * @param maxNum 最大消息数
     * @param begin from when
     * @param end to when
     * @return QueryResult实例
     */
    QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
        final long end) throws MQClientException, InterruptedException;

    /**
     * 通过消息id获取MessageExt
     *
     * @param topic 消息主题
     * @param 消息的messageId
     * @return {@code MessageExt}实例
     */
    MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

}