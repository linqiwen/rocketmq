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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * broker的消费者管理器
 */
public class ConsumerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 通道过期的超时时间，120秒
     */
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    /**
     * key：消费者组名称，value:消费者组信息
     */
    private final ConcurrentMap<String/* Group */, ConsumerGroupInfo> consumerTable =
        new ConcurrentHashMap<String, ConsumerGroupInfo>(1024);
    /**
     * 消费者Ids改变监听器
     */
    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }

    /**
     * 根据group和clientId查询客户端通道信息
     *
     * @param group 消费组名称
     * @param clientId 客户端id
     * @return 客户端通道信息
     */
    public ClientChannelInfo findChannel(final String group, final String clientId) {
        //先根据组名称获取消费者组信息
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        //消费者组信息存在
        if (consumerGroupInfo != null) {
            //根据clientId查询客户端通道信息
            return consumerGroupInfo.findChannel(clientId);
        }
        //消费者通道信息不存在，返回空
        return null;
    }

    /**
     * 根据topic和group查询topic订阅数据
     *
     * @param group 消费组名称
     * @param topic 主题
     * @return topic订阅数据
     */
    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        //先根据组名称获取消费者组信息
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        //消费者组信息存在
        if (consumerGroupInfo != null) {
            //获取topic的订阅数据
            return consumerGroupInfo.findSubscriptionData(topic);
        }

        return null;
    }

    /**
     * 根据组名称获取消费者组信息
     *
     * @param group 消费者组
     * @return 消费者组信息
     */
    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return this.consumerTable.get(group);
    }

    /**
     * 获取当前组订阅的数目
     */
    public int findSubscriptionDataCount(final String group) {
        //先根据组名称获取消费者组信息
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        //消费者组信息存在
        if (consumerGroupInfo != null) {
            //返回当前组订阅的数目
            return consumerGroupInfo.getSubscriptionTable().size();
        }

        return 0;
    }

    /**
     * 通道关闭事件
     *
     * @param remoteAddr 要关闭通道所对应的远程地址
     * @param channel 要关闭的通道
     */
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        //遍历所有消费者组的订阅信息
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            //获取到消费者组的订阅信息
            ConsumerGroupInfo info = next.getValue();
            //将关闭的通道从消费者组的消费者通道列表中移除
            boolean removed = info.doChannelCloseEvent(remoteAddr, channel);
            if (removed) {
                //关闭的通道从消费者组通道列表中移除,消费者组通道列表中无其他通道，将消费者的订阅信息从consumerTable中移除
                if (info.getChannelInfoTable().isEmpty()) {
                    //移除消费者组订阅信息
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        log.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                            next.getKey());
                        //消费者组注销事件处理
                        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, next.getKey());
                    }
                }
                //消费者组中的一些消费者发生了变化处理
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, next.getKey(), info.getAllChannel());
            }
        }
    }

    /**
     * 注册消费者
     *
     * @param group 消费者组
     * @param clientChannelInfo 消费者客户端通道信息
     * @param consumeType 消费类型
     * @param messageModel 消息模式
     * @param consumeFromWhere 从哪里开始进行消费
     * @param subList 当前组订阅的topic数据
     * @param isNotifyConsumerIdsChangedEnable 消费者id改变通知开关
     * @return 注册消费者是否成功
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {

        //先根据组名称获取消费者组信息
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        //如果当前消费组信息不存在
        if (null == consumerGroupInfo) {
            //创建新的消费者组信息
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            //将新的消费者信息加到consumerTable中，返回旧的ConsumerGroupInfo，这里存在竞态条件，可能存在多个线程同时对同一个组信息进行插入
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            //如果存在组信息获取原来的组信息，否则组信息为新的消费者组信息
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        //插入新的消费者通道
        boolean r1 =
            consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
                consumeFromWhere);
        boolean r2 = consumerGroupInfo.updateSubscription(subList);

        if (r1 || r2) {
            //消费者id改变通知开关
            if (isNotifyConsumerIdsChangedEnable) {
                //消费者组中的一些消费者发生了变化处理
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }

        //消费者组注册事件处理
        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.REGISTER, group, subList);

        return r1 || r2;
    }

    /**
     * 注销消费者
     *
     * @param group 消费者组
     * @param clientChannelInfo 消费者客户端通道信息
     * @param isNotifyConsumerIdsChangedEnable 消费者id改变通知开关
     */
    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        boolean isNotifyConsumerIdsChangedEnable) {
        //先根据组名称获取消费者组信息
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        //消费者组信息不为空
        if (null != consumerGroupInfo) {
            //将通道从channelInfoTable中移除
            consumerGroupInfo.unregisterChannel(clientChannelInfo);
            //消费者组中不存在其他通道
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                //将消费组信息从consumerTable中移除
                ConsumerGroupInfo remove = this.consumerTable.remove(group);
                //移除的消费组信息存在，打印日志
                if (remove != null) {
                    log.info("unregister consumer ok, no any connection, and remove consumer group, {}", group);
                    //消费者组注销事件处理
                    this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, group);
                }
            }
            //消费者id改变通知开关
            if (isNotifyConsumerIdsChangedEnable) {
                //消费者组中的一些消费者发生了变化处理
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
    }

    /**
     * 遍历不活跃的消费者通道，从groupChannelTable中移除，并将其关闭
     * <p>
     *     broker和消费者建立的连接是长连接，会有对应的心跳，
     *     超过一段时间没有收到心跳，会从channelInfoTable中移除，并将其关闭
     * </p>
     */
    public void scanNotActiveChannel() {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        //遍历所有的消费组信息
        while (it.hasNext()) {
            //获取每个消费组信息
            Entry<String, ConsumerGroupInfo> next = it.next();
            //消费组名称
            String group = next.getKey();
            //消费组信息
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            //获取消费组中的所有消费者通道信息
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                consumerGroupInfo.getChannelInfoTable();

            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            //遍历消费组中的所有消费者通道信息
            while (itChannel.hasNext()) {
                //获取到每个通道条目
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                //获取每个通道的信息
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                //当前时间和消费者最新的更新时间的差值
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                //差值大于通道过期的超时时间，关闭通道。，将通道从channelInfoTable中移除
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    log.warn(
                        "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                        RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    //关闭通道，会由监听器监听通道关闭，发送消费者改变事件，消费者接收到远程改变事件，重新分配队列和消费者
                    RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                    itChannel.remove();
                }
            }
            //如果消费组中没有其他通道，消费组从consumerTable中移除
            if (channelInfoTable.isEmpty()) {
                log.warn(
                    "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                    group);
                it.remove();
            }
        }
    }

    /**
     * 查看topic被哪些消费者组进行消费
     *
     * @param topic 主题
     * @return 消费此topic的消费组
     */
    public HashSet<String> queryTopicConsumeByWho(final String topic) {
        HashSet<String> groups = new HashSet<>();
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        //遍历所有的消费组
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> entry = it.next();
            ConcurrentMap<String, SubscriptionData> subscriptionTable =
                entry.getValue().getSubscriptionTable();
            //判断消费组中的订阅数据是否有此topic的，存在的话，将此group加入到集合中
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey());
            }
        }
        return groups;
    }
}
