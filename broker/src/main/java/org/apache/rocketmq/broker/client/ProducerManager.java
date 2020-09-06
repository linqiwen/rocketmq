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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * broker的生产者管理器
 */
public class ProducerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 锁超时时间，默认3秒
     */
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    /**
     * 通道过期的超时时间，120秒
     */
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    /**
     * 获取可用通道的重试次数
     */
    private static final int GET_AVALIABLE_CHANNEL_RETRY_COUNT = 3;
    /**
     * groupChannelTable操作锁
     */
    private final Lock groupChannelLock = new ReentrantLock();
    /**
     * key：组名称，value：<key:通道，value:客户端通道信息>
     */
    private final HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> groupChannelTable =
        new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
    /**
     * 正原子计数器，平均的获取可用通道
     */
    private PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();
    public ProducerManager() {
    }

    /**
     * 获取所有组的通道信息
     *
     * @param 所有组的通道信息
     */
    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable =
            new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    newGroupChannelTable.putAll(groupChannelTable);
                } finally {
                    groupChannelLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return newGroupChannelTable;
    }

    /**
     * 遍历生产者不活跃的通道，从groupChannelTable中移除，并将其关闭
     * <p>
     *     broker和生产者建立的连接是长连接，会有对应的心跳，
     *     超过一段时间没有收到心跳，会从groupChannelTable中移除，并将其关闭
     * </p>
     */
    public void scanNotActiveChannel() {
        try {
            //尝试加锁
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //遍历每个组的通道信息
                    for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                        .entrySet()) {
                        //获取组名称
                        final String group = entry.getKey();
                        //获取组里的所有通道
                        final HashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

                        Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
                        //遍历组内的通道信息
                        while (it.hasNext()) {
                            Entry<Channel, ClientChannelInfo> item = it.next();
                            // final Integer id = item.getKey();
                            //获取通道信息
                            final ClientChannelInfo info = item.getValue();
                            //当前时间和通道最新的更新时间差值
                            long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                            //差值大于通道过期的超时时间，即超过一定的时间没有收到的通道心跳
                            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                                //从groupChannelTable中移除
                                it.remove();
                                //打印日志
                                log.warn(
                                    "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                                    RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                                //关闭通道
                                RemotingUtil.closeChannel(info.getChannel());
                            }
                        }
                    }
                } finally {
                    //释放锁
                    this.groupChannelLock.unlock();
                }
            } else {
                //加锁超时
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    /**
     * 通道关闭事件
     * <p>
     *     从groupChannelTable中移除通道
     * </p>
     *
     * @param channel 要关闭的通道
     * @param remoteAddr 要关闭通道所对应的远程地址
     */
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            try {
                //尝试加锁
                if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {
                        //遍历每个组的通道信息
                        for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                            .entrySet()) {
                            //组名称
                            final String group = entry.getKey();
                            //获取组里的所有通道
                            final HashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                                entry.getValue();
                            //clientChannelInfoTable将通道移除
                            final ClientChannelInfo clientChannelInfo =
                                clientChannelInfoTable.remove(channel);
                            //打印日志
                            if (clientChannelInfo != null) {
                                log.info(
                                    "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                                    clientChannelInfo.toString(), remoteAddr, group);
                            }

                        }
                    } finally {
                        //释放锁
                        this.groupChannelLock.unlock();
                    }
                } else {
                    //加锁超时
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    /**
     * 注册生产者通道信息
     *
     * @param group 组名称
     * @param clientChannelInfo 生产者通道信息
     */
    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            ClientChannelInfo clientChannelInfoFound = null;

            //尝试加锁
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //获取组内的所有通道
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    //组内通道不存在，新建HashMap
                    if (null == channelTable) {
                        channelTable = new HashMap<>();
                        this.groupChannelTable.put(group, channelTable);
                    }

                    //通道不存在
                    clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
                    if (null == clientChannelInfoFound) {
                        //将通道信息加入到组内
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        log.info("new producer connected, group: {} channel: {}", group,
                            clientChannelInfo.toString());
                    }
                } finally {
                    //释放锁
                    this.groupChannelLock.unlock();
                }

                //通道已经存在，更新通道最近的更新时间
                if (clientChannelInfoFound != null) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            } else {
                log.warn("ProducerManager registerProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    /**
     * 注销生产者通道信息
     *
     * @param group 组名称
     * @param clientChannelInfo 生产者通道信息
     */
    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            //尝试加锁
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //获取组内的所有通道
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);

                    //如果组内存在通道
                    if (null != channelTable && !channelTable.isEmpty()) {
                        //将通道从组内移除
                        ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
                        //如果组内存在该通道，打印日志
                        if (old != null) {
                            log.info("unregister a producer[{}] from groupChannelTable {}", group,
                                clientChannelInfo.toString());
                        }

                        //如果组内不存在其他通道，从groupChannelTable将组移除
                        if (channelTable.isEmpty()) {
                            this.groupChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                        }
                    }
                } finally {
                    //释放锁
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager unregisterProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    /**
     * 获取可用通道
     */
    public Channel getAvaliableChannel(String groupId) {
        HashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(groupId);
        List<Channel> channelList = new ArrayList<Channel>();
        if (channelClientChannelInfoHashMap != null) {
            for (Channel channel : channelClientChannelInfoHashMap.keySet()) {
                channelList.add(channel);
            }
            int size = channelList.size();
            if (0 == size) {
                log.warn("Channel list is empty. groupId={}", groupId);
                return null;
            }

            int index = positiveAtomicCounter.incrementAndGet() % size;
            Channel channel = channelList.get(index);
            int count = 0;
            boolean isOk = channel.isActive() && channel.isWritable();
            while (count++ < GET_AVALIABLE_CHANNEL_RETRY_COUNT) {
                if (isOk) {
                    return channel;
                }
                index = (++index) % size;
                channel = channelList.get(index);
                isOk = channel.isActive() && channel.isWritable();
            }
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
            return null;
        }
        return null;
    }
}
