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
package org.apache.rocketmq.client.impl.consumer;

import java.util.Set;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 消费者内部接口
 */
public interface MQConsumerInner {

    /**
     * 消费组名称
     */
    String groupName();

    /**
     * 消息模式
     */
    MessageModel messageModel();

    /**
     * 消费类型
     */
    ConsumeType consumeType();

    /**
     * 消费位置
     */
    ConsumeFromWhere consumeFromWhere();

    /**
     * 消费者的订阅数据
     */
    Set<SubscriptionData> subscriptions();

    /**
     * 重新分配
     */
    void doRebalance();

    /**
     * 持久化消费偏移量
     */
    void persistConsumerOffset();

    /**
     * 更新主题的订阅信息
     *
     * @param topic 主题
     * @param info 消息队列列表
     */
    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    /**
     * 判断主题的订阅数据需要更新
     *
     * @param topic 主题
     * @return {@code true}主题的订阅数据需要更新
     */
    boolean isSubscribeTopicNeedUpdate(final String topic);

    boolean isUnitMode();

    /**
     * 消费者的运行信息
     */
    ConsumerRunningInfo consumerRunningInfo();
}
