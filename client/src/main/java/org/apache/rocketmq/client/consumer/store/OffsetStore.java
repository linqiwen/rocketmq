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
package org.apache.rocketmq.client.consumer.store;

import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 偏移量存储接口
 */
public interface OffsetStore {
    /**
     * 加载
     */
    void load() throws MQClientException;

    /**
     * 更新偏移量,将其存储在内存中
     *
     * @param mq 消息队列
     * @param offset 偏移量
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * 从本地存储或Broker获取偏移量
     *
     * @return 所获取的偏移量
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * 持久化所有偏移量,可以在本地存储或远程 name server
     *
     * @param mqs 消息队列集合，需要持久化偏移量的消息队列集合
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * 持久化偏移量,可以在本地存储或远程 name server
     *
     * @param mq 消息队列，需要持久化偏移量的消息队列
     */
    void persist(final MessageQueue mq);

    /**
     * 移除消息队列的偏移量
     */
    void removeOffset(MessageQueue mq);

    /**
     * 克隆给定主题偏移表
     *
     * @param topic 主题
     * @return 给定主题偏移表克隆，key:消息队列，value:偏移量
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * 更新消费偏移量到broker
     *
     * @param mq 消息队列
     * @param offset 偏移量
     * @param isOneway 是否是单向的，不需要管处理返回结果
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;
}
