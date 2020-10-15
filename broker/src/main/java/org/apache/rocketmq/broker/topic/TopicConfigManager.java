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
package org.apache.rocketmq.broker.topic;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;

/**
 * 主题配置管理器
 */
public class TopicConfigManager extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 锁超时时间
     */
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    /**
     * 锁
     */
    private transient final Lock lockTopicConfigTable = new ReentrantLock();

    /**
     * key:主题，value:主题配置文件
     */
    private final ConcurrentMap<String, TopicConfig> topicConfigTable =
        new ConcurrentHashMap<String, TopicConfig>(1024);
    /**
     * 数据版本
     */
    private final DataVersion dataVersion = new DataVersion();
    /**
     * 系统topic列表
     */
    private final Set<String> systemTopicList = new HashSet<String>();
    /**
     * broker控制器
     */
    private transient BrokerController brokerController;

    public TopicConfigManager() {
    }

    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        {
            //自我测试主题
            // MixAll.SELF_TEST_TOPIC
            String topic = MixAll.SELF_TEST_TOPIC;
            //测试主题配置
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            //设置读消息队列数量1
            topicConfig.setReadQueueNums(1);
            //设置写消息队列数量1
            topicConfig.setWriteQueueNums(1);
            //将主题和主题配置文件加入到topicConfigTable中
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC
            //判断自动创建开关是否打开
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                String topic = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC;
                TopicConfig topicConfig = new TopicConfig(topic);
                this.systemTopicList.add(topic);
                topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig()
                    .getDefaultTopicQueueNums());
                topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig()
                    .getDefaultTopicQueueNums());
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
                topicConfig.setPerm(perm);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            // MixAll.BENCHMARK_TOPIC
            //基准测试主题
            String topic = MixAll.BENCHMARK_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1024);
            topicConfig.setWriteQueueNums(1024);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName();
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            String topic = this.brokerController.getBrokerConfig().getBrokerName();
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getBrokerConfig().isBrokerTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // MixAll.OFFSET_MOVED_EVENT
            String topic = MixAll.OFFSET_MOVED_EVENT;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            if (this.brokerController.getBrokerConfig().isTraceTopicEnable()) {
                String topic = this.brokerController.getBrokerConfig().getMsgTraceTopicName();
                TopicConfig topicConfig = new TopicConfig(topic);
                this.systemTopicList.add(topic);
                topicConfig.setReadQueueNums(1);
                topicConfig.setWriteQueueNums(1);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
    }

    /**
     * 判断是否系统主题
     *
     * @param topic 主题
     * @return {@code true}系统主题
     */
    public boolean isSystemTopic(final String topic) {
        return this.systemTopicList.contains(topic);
    }

    /**
     * 获取系统主题列表
     *
     * @return 系统主题列表
     */
    public Set<String> getSystemTopic() {
        return this.systemTopicList;
    }

    /**
     * 判断主题是否能够发送消息
     *
     * @param topic 主题
     */
    public boolean isTopicCanSendMessage(final String topic) {
        return !topic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC);
    }

    /**
     * 根据主题获取主题配置
     *
     * @param topic 主题
     * @return 主题的配置
     */
    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }

    /**
     * 在发送消息方法中创建主题
     *
     * @param topic 主题
     * @param defaultTopic 默认主题
     * @param remoteAddress 远程地址
     * @param clientDefaultTopicQueueNums 客户端默认主题队列数量
     * @param topicSysFlag 主题系统标识
     * @return 主题配置信息
     */
    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
        final String remoteAddress, final int clientDefaultTopicQueueNums, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            //尝试加锁
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //根据主题获取主题的配置文件
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        //主题的配置文件存在，直接返回主题的配置文件
                        return topicConfig;

                    //根据默认主题获取默认主题的配置文件
                    TopicConfig defaultTopicConfig = this.topicConfigTable.get(defaultTopic);
                    if (defaultTopicConfig != null) {
                        if (defaultTopic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                            //如果默认主题是AUTO_CREATE_TOPIC_KEY_TOPIC，并且autoCreateTopicEnable为false，默认主题配置权限设置PERM_READ和PERM_WRITE
                            if (!this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                                defaultTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
                            }
                        }

                        //是否有权限创建主题
                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            //创建新的主题配置
                            topicConfig = new TopicConfig(topic);

                            //获取主题的队列数量，客户端的主题数量大于默认主题配置的写队列数量，设置为默认的主题配置的写队列数量，否则为客户端的主题数量
                            int queueNums =
                                clientDefaultTopicQueueNums > defaultTopicConfig.getWriteQueueNums() ? defaultTopicConfig
                                    .getWriteQueueNums() : clientDefaultTopicQueueNums;

                            //队列数量小于0，设置为0
                            if (queueNums < 0) {
                                queueNums = 0;
                            }
                            //设置读队列数量
                            topicConfig.setReadQueueNums(queueNums);
                            //设置写队列数量
                            topicConfig.setWriteQueueNums(queueNums);
                            //获取权限值
                            int perm = defaultTopicConfig.getPerm();
                            //移除PERM_INHERIT权限
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            //设置系统
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            //主题过滤类型，单标签、多标签
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        } else {
                            //如果默认主题没有PERM_INHERIT权限，打印日志
                            log.warn("Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]",
                                defaultTopic, defaultTopicConfig.getPerm(), remoteAddress);
                        }
                    } else {
                        //如果没有默认主题打印日志
                        log.warn("Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                            defaultTopic, remoteAddress);
                    }

                    if (topicConfig != null) {
                        log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]",
                            defaultTopic, topicConfig, remoteAddress);

                        //将主题的配置加入到topicConfigTable中
                        this.topicConfigTable.put(topic, topicConfig);
                        //数据的下一个版本
                        this.dataVersion.nextVersion();

                        createNew = true;
                        //将内容进行持久化
                        this.persist();
                    }
                } finally {
                    //释放锁
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        if (createNew) {
            //如果主题是新创建的
            this.brokerController.registerBrokerAll(false, true,true);
        }

        return topicConfig;
    }

    public TopicConfig createTopicInSendMessageBackMethod(
        final String topic,
        final int clientDefaultTopicQueueNums,
        final int perm,
        final int topicSysFlag) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null)
            return topicConfig;

        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(topicSysFlag);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(topic, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true,true);
        }

        return topicConfig;
    }

    public void updateTopicUnitFlag(final String topic, final boolean unit) {

        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (unit) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true,true);
        }
    }

    public void updateTopicUnitSubFlag(final String topic, final boolean hasUnitSub) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (hasUnitSub) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true,true);
        }
    }

    public void updateTopicConfig(final TopicConfig topicConfig) {
        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }

    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {

        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            for (String topic : orderTopics) {
                TopicConfig topicConfig = this.topicConfigTable.get(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            for (Map.Entry<String, TopicConfig> entry : this.topicConfigTable.entrySet()) {
                String topic = entry.getKey();
                if (!orderTopics.contains(topic)) {
                    TopicConfig topicConfig = entry.getValue();
                    if (topicConfig.isOrder()) {
                        topicConfig.setOrder(false);
                        isChange = true;
                        log.info("update order topic config, topic={}, order={}", topic, false);
                    }
                }
            }

            if (isChange) {
                this.dataVersion.nextVersion();
                this.persist();
            }
        }
    }

    /**
     * 判断主题是否有序
     *
     * @param topic 主题
     * @return {@code true}主题有序
     */
    public boolean isOrderTopic(final String topic) {
        //根据主题获取主题的配置文件
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        //配置文件为空返回false
        if (topicConfig == null) {
            return false;
        } else {
            //返回主题是否有序
            return topicConfig.isOrder();
        }
    }

    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: {}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete topic config failed, topic: {} not exists", topic);
        }
    }

    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
            if (topicConfigSerializeWrapper != null) {
                this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());
                this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
            }
        }
    }

    /**
     * 对主题配置进行编码
     */
    public String encode(final boolean prettyFormat) {
        //创建主题配置的序列化包装类
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        //设置主题配置文件列表
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        //设置数据版本
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        //主题配置的序列化包装类编码成json串
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        Iterator<Entry<String, TopicConfig>> it = tcs.getTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicConfig> next = it.next();
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }
}
