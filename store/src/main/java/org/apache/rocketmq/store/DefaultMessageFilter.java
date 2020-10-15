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

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 默认的消息过滤器
 */
public class DefaultMessageFilter implements MessageFilter {

    /**
     * 订阅数据
     */
    private SubscriptionData subscriptionData;

    public DefaultMessageFilter(final SubscriptionData subscriptionData) {
        this.subscriptionData = subscriptionData;
    }

    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (null == tagsCode || null == subscriptionData) {
            return true;
        }

        //如果是类过滤模式
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        //如果是订阅所有
        return subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)
                //或者是订阅的主题数据包含消息的tagsCode
            || subscriptionData.getCodeSet().contains(tagsCode.intValue());
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        return true;
    }
}
