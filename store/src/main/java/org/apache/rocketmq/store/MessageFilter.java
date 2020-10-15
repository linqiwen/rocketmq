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
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 消息过滤器
 */
public interface MessageFilter {

    /**
     * 通过标签hash值或过滤位图，当消息接收并存储在消费队列ext中时计算
     *
     * @param tagsCode tagsCode
     * @param cqExtUnit 扩展消费单元队列
     */
    boolean isMatchedByConsumeQueue(final Long tagsCode,
        final ConsumeQueueExt.CqExtUnit cqExtUnit);

    /**
     * 根据存储在commit log中的消息内容进行匹配.
     * <br>{@code msgBuffer} and {@code properties} 不全部为null.如果在store中调用,
     * {@code properties} 为null;如果在{@code PullRequestHoldService}调用, {@code msgBuffer}为null.
     *
     * @param msgBuffer 在commit log的消息缓冲区, 可能会null如果不是在store.
     * @param properties 消息属性, 如果为null，应该从缓冲区解码.
     */
    boolean isMatchedByCommitLog(final ByteBuffer msgBuffer,
        final Map<String, String> properties);
}
