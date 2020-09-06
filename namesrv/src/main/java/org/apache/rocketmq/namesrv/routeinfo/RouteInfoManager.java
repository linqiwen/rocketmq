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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * 路由信息管理器
 */
public class RouteInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
    /**
     * 分布式锁
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * key:主题，value:队列数据列表
     */
    private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;
    /**
     * key:broker名称，value:broker数据
     */
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    /**
     * key:集群名称，value:broker名称列表
     */
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
    /**
     * key:broker地址，value:broker信息
     */
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
    /**
     * key:broker地址，value:过滤服务列表
     */
    private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
        this.brokerAddrTable = new HashMap<String, BrokerData>(128);
        this.clusterAddrTable = new HashMap<String, Set<String>>(32);
        this.brokerLiveTable = new HashMap<String, BrokerLiveInfo>(256);
        this.filterServerTable = new HashMap<String, List<String>>(256);
    }

    /**
     * 获取所有的集群、包括集群-》broker名称和broker名称-》所有的broker地址列表
     *
     * @return 字节数组
     */
    public byte[] getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper.encode();
    }

    /**
     * 删除topic
     *
     * @param topic 主题
     */
    public void deleteTopic(final String topic) {
        try {
            try {
                //加可中断写锁
                this.lock.writeLock().lockInterruptibly();
                //主题从topicQueueTable中移除
                this.topicQueueTable.remove(topic);
            } finally {
                //释放锁
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    /**
     * 获取所有的主题名称列表字节数组
     *
     * @return 主题名称列表字节数组
     */
    public byte[] getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                //加可中断读锁
                this.lock.readLock().lockInterruptibly();
                //获取所有的主题名称
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            } finally {
                //释放锁
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    /**
     * 注册broker信息
     *
     * @param clusterName 集群名称
     * @param brokerAddr broker地址
     * @param brokerName broker名称
     * @param brokerId brokerId
     * @param haServerAddr 主broker地址
     * @param topicConfigWrapper 主题配置信息
     * @param filterServerList 过滤服务列表
     * @param channel broker通道
     * @return 注册broker的结果
     */
    public RegisterBrokerResult registerBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            try {
                //加可中断写锁
                this.lock.writeLock().lockInterruptibly();
                //获取集群的所有broker名称集合
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (null == brokerNames) {
                    //集群不存在broker，创建新的broker名称集合
                    brokerNames = new HashSet<String>();
                    //将broker名称集合和集群名称关联
                    this.clusterAddrTable.put(clusterName, brokerNames);
                }
                //将broker名称加到broker名称集合中
                brokerNames.add(brokerName);
                //首次注册
                boolean registerFirst = false;
                //根据broker名称获取broker数据，主从broker名称相同，brokerId不同
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                //broker数据不存在第一次注册
                if (null == brokerData) {
                    //broker第一次注册
                    registerFirst = true;
                    //根据集群名称、broker名称、broker地址列表创建broker数据
                    brokerData = new BrokerData(clusterName, brokerName, new HashMap<Long, String>());
                    //将broker名称和broker数据进行关联
                    this.brokerAddrTable.put(brokerName, brokerData);
                }
                //获取该broker数据
                Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
                //从机切换到主: 首先删除namesrv中的<1, IP:PORT>, 然后添加 <0, IP:PORT>
                //同一个IP:PORT 在brokerAddrTable中只能有一个记录
                Iterator<Entry<Long, String>> it = brokerAddrsMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, String> item = it.next();
                    //从机切换到主，需要将从机旧的brokerId移除
                    if (null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey()) {
                        it.remove();
                    }
                }

                //将新的brokerId和broker地址加入进去
                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                //如果是注册新的broker，或者从broker切换到主，registerFirst为true
                registerFirst = registerFirst || (null == oldAddr);

                //如果主题配置不为空，并且注册的broker为主broker
                if (null != topicConfigWrapper
                    && MixAll.MASTER_ID == brokerId) {
                    //判断主的broker的数据版本是否和传入进来的主题配置数据版本一致，数据版本不一致或首次注册
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion())
                        || registerFirst) {
                        //获取所有主题的配置信息
                        ConcurrentMap<String, TopicConfig> tcTable =
                            topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            //遍历每个topic的配置信息
                            for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                                //创建或更新topic的队列数据
                                this.createAndUpdateQueueData(brokerName, entry.getValue());
                            }
                        }
                    }
                }
                //构造新的broker现场信息放入到brokerLiveTable中
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr,
                    new BrokerLiveInfo(
                        System.currentTimeMillis(),
                        topicConfigWrapper.getDataVersion(),
                        channel,
                        haServerAddr));
                //如果前一个broker现场信息为空，打印日志
                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
                }
                //如果过滤服务列表不为null
                if (filterServerList != null) {
                    //如果过滤服务列表为空列表
                    if (filterServerList.isEmpty()) {
                        //将brokerAddr中的过滤服务列表移除
                        this.filterServerTable.remove(brokerAddr);
                    } else {
                        //否则将brokerAddr和过滤服务列表建立连接
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                /**
                 * 如果当前brokerId不是主broker
                 */
                if (MixAll.MASTER_ID != brokerId) {
                    //获取主的broker地址
                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        //获取主的broker现场信息
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            } finally {
                //释放锁
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        }
        //返回结果
        return result;
    }

    /**
     * 判断broker的主题配置文件是否改变
     *
     * @param brokerAddr broker地址
     * @param dataVersion 主题配置文件新的数据版本
     * @return {@code true}broker的主题配置文件改变
     */
    public boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        //根据broker地址获取数据版本
        DataVersion prev = queryBrokerTopicConfig(brokerAddr);
        //broker的数据版本为空或者和原来的不相等
        return null == prev || !prev.equals(dataVersion);
    }

    /**
     * 查询broker的原本主题配置数据版本
     *
     * @param brokerAddr broker地址
     * @return 数据版本
     */
    public DataVersion queryBrokerTopicConfig(final String brokerAddr) {
        //获取broker当前的现场信息
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            //返回数据版本
            return prev.getDataVersion();
        }
        return null;
    }

    /**
     * 更新broker现场信息的时间戳
     *
     * @param brokerAddr broker地址
     */
    public void updateBrokerInfoUpdateTimestamp(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            //设置最近的更新时间为当前时间戳
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        //队列数据
        QueueData queueData = new QueueData();
        //broker名称
        queueData.setBrokerName(brokerName);
        //写队列数目
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        //读队列数目
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        //设置topic权限
        queueData.setPerm(topicConfig.getPerm());
        //设置主题同步标识
        queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());

        //获取topic的队列数据列表
        List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataList) {
            queueDataList = new LinkedList<QueueData>();
            queueDataList.add(queueData);
            //将topic名称和队列数据列表关联
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            boolean addNewOne = true;

            Iterator<QueueData> it = queueDataList.iterator();
            //遍历每个topic队列数据
            while (it.hasNext()) {
                QueueData qd = it.next();
                //topic的数据brokerName相同
                if (qd.getBrokerName().equals(brokerName)) {
                    //并且队列数据相同，不需要新建
                    if (qd.equals(queueData)) {
                        addNewOne = false;
                    } else {
                        //如果同一个brokerName的topic的队列数据不相同，将其旧的移除
                        log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), qd,
                            queueData);
                        it.remove();
                    }
                }
            }
            //如果需要新增，将topic的队列数据新增进去
            if (addNewOne) {
                queueDataList.add(queueData);
            }
        }
    }

    /**
     * 擦除broker的写入权限在锁的条件下
     *
     * @param brokerName broker名称
     * @return 总共擦除的队列数据
     */
    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                //加可中断写锁
                this.lock.writeLock().lockInterruptibly();
                //擦除broker的写入权限
                return wipeWritePermOfBroker(brokerName);
            } finally {
                //释放锁
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }

    private int wipeWritePermOfBroker(final String brokerName) {
        //总共擦除的队列数据
        int wipeTopicCnt = 0;
        //遍历所有主题的队列数据列表
        Iterator<Entry<String, List<QueueData>>> itTopic = this.topicQueueTable.entrySet().iterator();
        while (itTopic.hasNext()) {
            //每个主题和主题的队列数据列表
            Entry<String, List<QueueData>> entry = itTopic.next();
            //主题的队列数据列表
            List<QueueData> qdList = entry.getValue();

            //遍历主题的队列数据列表
            Iterator<QueueData> it = qdList.iterator();
            while (it.hasNext()) {
                //每个队列数据
                QueueData qd = it.next();
                //如果和传入进来的brokerName相等，队列数据的擦除写权限
                if (qd.getBrokerName().equals(brokerName)) {
                    //获取队列数据的权限
                    int perm = qd.getPerm();
                    //擦除写权限
                    perm &= ~PermName.PERM_WRITE;
                    qd.setPerm(perm);
                    wipeTopicCnt++;
                }
            }
        }

        return wipeTopicCnt;
    }

    /**
     * 注销broker
     *
     * @param clusterName 集群名称
     * @param brokerAddr broker地址
     * @param brokerName broker名称
     * @param brokerId broker地址
     */
    public void unregisterBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId) {
        try {
            try {
                //加可中断写锁
                this.lock.writeLock().lockInterruptibly();
                //移除broker的现场信息
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
                    brokerLiveInfo != null ? "OK" : "Failed",
                    brokerAddr
                );

                //移除broker的过滤列表
                this.filterServerTable.remove(brokerAddr);

                //是否移除brokerName，因为一个
                boolean removeBrokerName = false;
                //获取broker的broker数据
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                //如果broker数据不为空
                if (null != brokerData) {
                    //从brokerName的brokerId地址列表中移除此brokerId，主从brokerName相同，brokerId不同
                    String addr = brokerData.getBrokerAddrs().remove(brokerId);
                    //打印日志
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}",
                        addr != null ? "OK" : "Failed",
                        brokerAddr
                    );

                    //如果brokerName下没有其他的brokerId
                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        //将brokerName从brokerAddrTable地址中移除
                        this.brokerAddrTable.remove(brokerName);
                        //打印日志
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}",
                            brokerName
                        );
                        //brokerName从brokerAddrTable地址中移除
                        removeBrokerName = true;
                    }
                }

                //brokerName从brokerAddrTable地址中移除
                if (removeBrokerName) {
                    //获取集群下的所有brokerName列表
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    //brokerName列表不为空
                    if (nameSet != null) {
                        //将brokerName从集群下的brokerName列表移除
                        boolean removed = nameSet.remove(brokerName);
                        //打印日志
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}",
                            removed ? "OK" : "Failed",
                            brokerName);

                        //如果集群下没有其他的brokerName
                        if (nameSet.isEmpty()) {
                            //将clusterName从clusterAddrTable中移除
                            this.clusterAddrTable.remove(clusterName);
                            //打印日志
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}",
                                clusterName
                            );
                        }
                    }
                    //移除brokerName的所有topic
                    this.removeTopicByBrokerName(brokerName);
                }
            } finally {
                //释放锁
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            //出现异常打印日志
            log.error("unregisterBroker Exception", e);
        }
    }

    /**
     * 移除brokerName下的所有topic
     *
     * @param brokerName broker名称
     */
    private void removeTopicByBrokerName(final String brokerName) {
        //遍历所有的主题
        Iterator<Entry<String, List<QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            //获取到每个主题和主题的队列数据
            Entry<String, List<QueueData>> entry = itMap.next();

            //获取到主题
            String topic = entry.getKey();
            //获取到队列数据
            List<QueueData> queueDataList = entry.getValue();
            Iterator<QueueData> it = queueDataList.iterator();
            //遍历topic的每个队列数据
            while (it.hasNext()) {
                //获取到topic的队列数据
                QueueData qd = it.next();
                //topic的队列数据的brokerName和移除的brokerName相等
                if (qd.getBrokerName().equals(brokerName)) {
                    //打印日志
                    log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, qd);
                    //移除队列数据
                    it.remove();
                }
            }

            //如果主题下没有其他队列数据
            if (queueDataList.isEmpty()) {
                //打印日志
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
                //将主题从topicQueueTable中移除
                itMap.remove();
            }
        }
    }

    /**
     * 获取主题路由数据
     *
     * @param topic 主题
     * @return 主题路由数据
     */
    public TopicRouteData pickupTopicRouteData(final String topic) {
        //创建一个主题路由数据对象
        TopicRouteData topicRouteData = new TopicRouteData();
        //找到队列数据标识
        boolean foundQueueData = false;
        //找到broker队列数据标识
        boolean foundBrokerData = false;
        //broker名称集合
        Set<String> brokerNameSet = new HashSet<String>();
        //broker数据集合
        List<BrokerData> brokerDataList = new LinkedList<BrokerData>();
        //将broker数据集合加入到路由数据中
        topicRouteData.setBrokerDatas(brokerDataList);
        //key->brokerAddr，value->过滤服务列表
        HashMap<String, List<String>> filterServerMap = new HashMap<String, List<String>>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {
                //加可中断读锁
                this.lock.readLock().lockInterruptibly();
                //获取主题的队列数据列表
                List<QueueData> queueDataList = this.topicQueueTable.get(topic);
                //队列数据不为空
                if (queueDataList != null) {
                    //将队列数据加入到主题路由数据中
                    topicRouteData.setQueueDatas(queueDataList);
                    foundQueueData = true;

                    Iterator<QueueData> it = queueDataList.iterator();
                    //遍历所有的队列数据
                    while (it.hasNext()) {
                        QueueData qd = it.next();
                        //将队列数据中的brokerName加入到brokerNameSet中
                        brokerNameSet.add(qd.getBrokerName());
                    }

                    //遍历brokerName集合
                    for (String brokerName : brokerNameSet) {
                        //获取每个brokerName的broker数据
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (null != brokerData) {
                            //copy一份新的broker数据
                            BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(), brokerData.getBrokerName(), (HashMap<Long, String>) brokerData
                                .getBrokerAddrs().clone());
                            //将克隆的broker数据加到brokerDataList中
                            brokerDataList.add(brokerDataClone);
                            foundBrokerData = true;
                            //将所有的broker地址的过滤服务列表加到filterServerMap中
                            for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                                filterServerMap.put(brokerAddr, filterServerList);
                            }
                        }
                    }
                }
            } finally {
                //释放锁
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            //出现异常打印日志
            log.error("pickupTopicRouteData Exception", e);
        }
        //打印日志
        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        //broker数据、队列数据都存在
        if (foundBrokerData && foundQueueData) {
            //返回topic的队列数据
            return topicRouteData;
        }
        //否则返回null
        return null;
    }

    /**
     * 扫描不活跃的broker，120秒为收到broker的心跳包，关闭broker通道
     */
    public void scanNotActiveBroker() {
        //遍历所有的broker信息
        Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            //获取到每个broker信息
            Entry<String, BrokerLiveInfo> next = it.next();
            //获取broker最近的更新时间
            long last = next.getValue().getLastUpdateTimestamp();
            //超过120秒没有收到broker心跳
            if ((last + BROKER_CHANNEL_EXPIRED_TIME) < System.currentTimeMillis()) {
                //关闭broker通道
                RemotingUtil.closeChannel(next.getValue().getChannel());
                //将broker从brokerLiveTable中移除
                it.remove();
                log.warn("The broker channel expired, {} {}ms", next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
                //broker通道关闭
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());
            }
        }
    }

    /**
     * 通道关闭
     *
     * @param remoteAddr 远程地址
     * @param channel 通道
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerAddrFound = null;
        if (channel != null) {
            try {
                try {
                    //尝试加可中断读锁
                    this.lock.readLock().lockInterruptibly();
                    //遍历所有的broker信息
                    Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable =
                        this.brokerLiveTable.entrySet().iterator();
                    while (itBrokerLiveTable.hasNext()) {
                        //获取到broker信息
                        Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                        //broker信息中通道和传入进来的通道相等
                        if (entry.getValue().getChannel() == channel) {
                            //brokerAddrFound置为此broker地址
                            brokerAddrFound = entry.getKey();
                            //退出
                            break;
                        }
                    }
                } finally {
                    //释放锁
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                //出现异常打印日志
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (null == brokerAddrFound) {
            //如果brokerAddrFound未找到，将brokerAddrFound设置为传入进来的remoteAddr地址
            brokerAddrFound = remoteAddr;
        } else {
            //存在，打印日志
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        //brokerAddrFound不为空串
        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {

            try {
                try {
                    //加可中断写锁
                    this.lock.writeLock().lockInterruptibly();
                    //移除broker信息
                    this.brokerLiveTable.remove(brokerAddrFound);
                    //移除broker的过滤服务列表
                    this.filterServerTable.remove(brokerAddrFound);

                    //找寻到brokerName
                    String brokerNameFound = null;
                    //是否移除brokerName
                    boolean removeBrokerName = false;
                    Iterator<Entry<String, BrokerData>> itBrokerAddrTable =
                        this.brokerAddrTable.entrySet().iterator();
                    //遍历brokerName列表，主从的brokerName相同，brokerId不同
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                        //获取每个brokerName对应的BrokerData
                        BrokerData brokerData = itBrokerAddrTable.next().getValue();

                        //遍历brokerName里的所有broker地址
                        Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (it.hasNext()) {
                            //broker地址
                            Entry<Long, String> entry = it.next();
                            //brokerId
                            Long brokerId = entry.getKey();
                            //broker地址
                            String brokerAddr = entry.getValue();
                            //匹配broker地址
                            if (brokerAddr.equals(brokerAddrFound)) {
                                //赋值brokerName
                                brokerNameFound = brokerData.getBrokerName();
                                //将broker地址从brokerName中的地址列表中移除
                                it.remove();
                                //打印日志
                                log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed",
                                    brokerId, brokerAddr);
                                //退出
                                break;
                            }
                        }

                        //如果brokerName所对应的broker地址为空，即没有其他的broker
                        if (brokerData.getBrokerAddrs().isEmpty()) {
                            //移除brokerName置为true
                            removeBrokerName = true;
                            //brokerName从brokerAddrTable中移除
                            itBrokerAddrTable.remove();
                            log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed",
                                brokerData.getBrokerName());
                        }
                    }

                    //brokerNameFound不为空，brokerName移除
                    if (brokerNameFound != null && removeBrokerName) {
                        Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                        //遍历所有集群
                        while (it.hasNext()) {
                            //集群和broker名称集合的关联元素
                            Entry<String, Set<String>> entry = it.next();
                            //集群名称
                            String clusterName = entry.getKey();
                            //集群的broker名称集合
                            Set<String> brokerNames = entry.getValue();
                            //集群的broker名称集合移除
                            boolean removed = brokerNames.remove(brokerNameFound);
                            if (removed) {
                                //移除成功打印日志
                                log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed",
                                    brokerNameFound, clusterName);
                                //集群的broker名称集合为空，集群里没有其他的broker
                                if (brokerNames.isEmpty()) {
                                    log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster",
                                        clusterName);
                                    //从集群列表中移除集群
                                    it.remove();
                                }

                                break;
                            }
                        }
                    }

                    if (removeBrokerName) {
                        //遍历所有的主题
                        Iterator<Entry<String, List<QueueData>>> itTopicQueueTable =
                            this.topicQueueTable.entrySet().iterator();
                        while (itTopicQueueTable.hasNext()) {
                            //主题和队列数据关联元素
                            Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
                            //主题
                            String topic = entry.getKey();
                            //主题的队列数据
                            List<QueueData> queueDataList = entry.getValue();

                            Iterator<QueueData> itQueueData = queueDataList.iterator();
                            //遍历所有的队列数据
                            while (itQueueData.hasNext()) {
                                //队列数据
                                QueueData queueData = itQueueData.next();
                                //brokerNameFound中的队列数据移除
                                if (queueData.getBrokerName().equals(brokerNameFound)) {
                                    //移除brokerNameFound中的队列数据
                                    itQueueData.remove();
                                    //打印日志
                                    log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed",
                                        topic, queueData);
                                }
                            }

                            //主题的队列数据为空
                            if (queueDataList.isEmpty()) {
                                //将主题从topicQueueTable中移除
                                itTopicQueueTable.remove();
                                log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed",
                                    topic);
                            }
                        }
                    }
                } finally {
                    //释放锁
                    this.lock.writeLock().unlock();
                }
            } catch (Exception e) {
                //出现异常打印日志
                log.error("onChannelDestroy Exception", e);
            }
        }
    }

    /**
     * 定期打印所有
     */
    public void printAllPeriodically() {
        try {
            try {
                //获取可中断读锁
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    //打印所有主题和主题队列数据
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    Iterator<Entry<String, List<QueueData>>> it = this.topicQueueTable.entrySet().iterator();
                    while (it.hasNext()) {
                        //主题和主题队列数据元素
                        Entry<String, List<QueueData>> next = it.next();
                        //打印每个主题和主题队列数据元素
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    //打印所有brokerName和broker数据
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        //brokerName和broker数据
                        Entry<String, BrokerData> next = it.next();
                        //打印每个brokerName和broker数据
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    //打印所有broker地址和broker信息
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
                    while (it.hasNext()) {
                        //broker地址和broker信息
                        Entry<String, BrokerLiveInfo> next = it.next();
                        //打印每个broker地址和broker信息
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    //打印所有集群名称和broker名称列表
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        //集群名称和broker名称列表
                        Entry<String, Set<String>> next = it.next();
                        //打印每个集群名称和broker名称列表
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                //释放可中断读锁
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    public byte[] getSystemTopicList() {
        //主题集合
        TopicList topicList = new TopicList();
        try {
            try {
                //获取可中断读锁
                this.lock.readLock().lockInterruptibly();
                for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                    //加入集群名称
                    topicList.getTopicList().add(entry.getKey());
                    //加入集群下的所有brokerName列表
                    topicList.getTopicList().addAll(entry.getValue());
                }
                //broker地址和broker数据映射不为空
                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Iterator<String> it = brokerAddrTable.keySet().iterator();
                    while (it.hasNext()) {
                        //获取broker数据
                        BrokerData bd = brokerAddrTable.get(it.next());
                        //获取所有broker地址
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                        if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            //设置broker地址
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                            break;
                        }
                    }
                }
            } finally {
                //释放锁
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            //出现异常打印日志
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    /**
     * 根据集群名称获取所有主题
     *
     * @param cluster 集群名称
     * @return 集群下的所有主题
     */
    public byte[] getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                //获取可中断读锁
                this.lock.readLock().lockInterruptibly();
                //获取集群下的所有broker名称集合
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                //遍历broker名称集合
                for (String brokerName : brokerNameSet) {
                    Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                    //遍历所有的主题和其队列数据
                    while (topicTableIt.hasNext()) {
                        //获取主题下的队列数据
                        Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                        String topic = topicEntry.getKey();
                        List<QueueData> queueDatas = topicEntry.getValue();
                        for (QueueData queueData : queueDatas) {
                            if (brokerName.equals(queueData.getBrokerName())) {
                                topicList.getTopicList().add(topic);
                                break;
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                    this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                        && TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                    this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                        && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                    this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                        && !TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag())
                        && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }
}

/**
 * broker现场信息
 */
class BrokerLiveInfo {
    /**
     * 最近的更新时间
     */
    private long lastUpdateTimestamp;
    /**
     * 数据版本
     */
    private DataVersion dataVersion;
    /**
     * broker通道
     */
    private Channel channel;
    /**
     * 主broker地址
     */
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel,
        String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
            + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}
