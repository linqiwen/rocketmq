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
package org.apache.rocketmq.client.consumer;

import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 同一个group下的消费者之间的消息队列分配算法策略
 */
public interface AllocateMessageQueueStrategy {

    /**
     * 通过消费id分配
     *
     * @param consumerGroup 当前消费者的group
     * @param currentCID 当前消费者id
     * @param mqAll 当前topic下的所有
     * @param cidAll 当前消费组下的所有消费者id
     * @return 根据策略分配的结果，即当前消费者分配到的队列
     */
    List<MessageQueue> allocate(
        final String consumerGroup,
        final String currentCID,
        final List<MessageQueue> mqAll,
        final List<String> cidAll
    );

    /**
     * 队列分配策略算法名称
     *
     * @return 策略算法
     */
    String getName();
}
