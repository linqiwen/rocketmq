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
package org.apache.rocketmq.client.impl.producer;

import java.util.Set;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;

/**
 * MQ生产者内部
 */
public interface MQProducerInner {
    Set<String> getPublishTopicList();

    boolean isPublishTopicNeedUpdate(final String topic);

    TransactionCheckListener checkListener();
    /**
     * 获取事务监听器
     *
     * @return 事务监听器
     */
    TransactionListener getCheckListener();

    void checkTransactionState(
        final String addr,
        final MessageExt msg,
        final CheckTransactionStateRequestHeader checkRequestHeader);

    /**
     * 更新主题的发布信息
     *
     * @param topic 主题
     * @param info 主题的发布信息
     */
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    boolean isUnitMode();
}
