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

import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;

/**
 * 推送消费者
 */
public interface MQPushConsumer extends MQConsumer {
    /**
     * 开启消费者
     */
    void start() throws MQClientException;

    /**
     * 关闭消费者
     */
    void shutdown();

    /**
     * 注册消息监听器
     */
    @Deprecated
    void registerMessageListener(MessageListener messageListener);

    /**
     * 注册并发消息监听器
     */
    void registerMessageListener(final MessageListenerConcurrently messageListener);

    /**
     * 注册有序消息监听器
     */
    void registerMessageListener(final MessageListenerOrderly messageListener);

    /**
     * 订阅一个主题
     *
     * @param topic 要订阅的主题
     * @param subExpression 订阅表达式.只支持如下 "tag1 || tag2 || tag3" <br> 如果
     * null 或者 * 表达式,意味着订阅此topic的所有消息
     */
    void subscribe(final String topic, final String subExpression) throws MQClientException;

    /**
     * This method will be removed in the version 5.0.0,because filterServer was removed,and method <code>subscribe(final String topic, final MessageSelector messageSelector)</code>
     * is recommended.
     *
     * 订阅一个主题
     *
     * @param fullClassName full class name,must extend org.apache.rocketmq.common.filter. MessageFilter
     * @param filterClassSource class source code,used UTF-8 file encoding,must be responsible for your code safety
     */
    @Deprecated
    void subscribe(final String topic, final String fullClassName,
        final String filterClassSource) throws MQClientException;

    /**
     * 订阅一个主题并且带有消息选择器.
     * <p>
     * 这个方法具有和 {@link #subscribe(String, String)}方法相同的能力,
     * 并且, 支持其他的消息选择器, 比如 {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}.
     * </p>
     * <p/>
     * <p>
     * Choose Tag: {@link MessageSelector#byTag(java.lang.String)}
     * </p>
     * <p/>
     * <p>
     * Choose SQL92: {@link MessageSelector#bySql(java.lang.String)}
     * </p>
     *
     * @param selector 消息选择器({@link MessageSelector}), 可以为null.
     */
    void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

    /**
     * 退订消费一些主题
     *
     * @param topic 消息主题
     */
    void unsubscribe(final String topic);

    /**
     * 动态更新消费者线程池的大小
     */
    void updateCorePoolSize(int corePoolSize);

    /**
     * 停止消费
     */
    void suspend();

    /**
     * 恢复消费
     */
    void resume();
}
