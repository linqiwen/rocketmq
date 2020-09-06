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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 默认消费者Ids改变监听器
 */
public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {
    /**
     * broker控制器
     */
    private final BrokerController brokerController;

    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 消费组事件处理
     *
     * @param group 消费组
     * @param event 事件
     * @param args 参数
     */
    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        //事件为空直接返回
        if (event == null) {
            return;
        }
        switch (event) {
            case CHANGE:
                //group中的Channel为空，直接返回
                if (args == null || args.length < 1) {
                    return;
                }
                //获取group中的所有通道
                List<Channel> channels = (List<Channel>) args[0];
                //通道不为空并且组中消费者改变事件通知开关打开
                if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    //往各个通道中发送通知消息
                    for (Channel chl : channels) {
                        //往chl通道发送通知消息
                        this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
                    }
                }
                break;
            case UNREGISTER:
                this.brokerController.getConsumerFilterManager().unRegister(group);
                break;
            case REGISTER:
                if (args == null || args.length < 1) {
                    return;
                }
                Collection<SubscriptionData> subscriptionDataList = (Collection<SubscriptionData>) args[0];
                this.brokerController.getConsumerFilterManager().register(group, subscriptionDataList);
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }
}
