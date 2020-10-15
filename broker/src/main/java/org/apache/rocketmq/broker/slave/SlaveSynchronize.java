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
package org.apache.rocketmq.broker.slave;

import java.io.IOException;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
/**
 * 从同步
 */
public class SlaveSynchronize {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * broker控制器
     */
    private final BrokerController brokerController;
    /**
     * 主broker地址
     */
    private volatile String masterAddr = null;

    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    /**
     * 同步所有
     */
    public void syncAll() {
        //同步主题配置
        this.syncTopicConfig();
        //同步消费者偏移量
        this.syncConsumerOffset();
        //同步延迟偏移量
        this.syncDelayOffset();
        //同步订阅组配置
        this.syncSubscriptionGroupConfig();
    }

    /**
     * 同步主题配置
     */
    private void syncTopicConfig() {
        //主broker地址
        String masterAddrBak = this.masterAddr;
        //主broker地址不等于自身
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                //获取所有的主题配置
                TopicConfigSerializeWrapper topicWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                    .equals(topicWrapper.getDataVersion())) {

                    //分配新的数据版本
                    this.brokerController.getTopicConfigManager().getDataVersion()
                        .assignNewOne(topicWrapper.getDataVersion());
                    //将原本的主题配置列表清空掉
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();
                    //将从主broker同步到主题配置设置进去
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                        .putAll(topicWrapper.getTopicConfigTable());
                    //将主题配置进行文件持久化
                    this.brokerController.getTopicConfigManager().persist();
                    //打印日志
                    log.info("Update slave topic config from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                //出现异常打印日志
                log.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 同步消费者偏移量
     */
    private void syncConsumerOffset() {
        //主broker地址
        String masterAddrBak = this.masterAddr;
        //主broker地址不等于自身
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                ConsumerOffsetSerializeWrapper offsetWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);
                //重新设置每个消费组对主题下的队列的消费偏移量
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                    .putAll(offsetWrapper.getOffsetTable());
                //将消费者偏移量进行文件持久化
                this.brokerController.getConsumerOffsetManager().persist();
                //出现异常打印日志
                log.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                //出现异常打印日志
                log.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 同步延迟偏移量
     */
    private void syncDelayOffset() {
        //主broker地址
        String masterAddrBak = this.masterAddr;
        //主broker地址不等于自身
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                String delayOffset =
                    this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {

                    //获取保存日志数据的根目录
                    String fileName =
                        StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                            .getMessageStoreConfig().getStorePathRootDir());
                    try {
                        //将延迟偏移量持久化到文件中
                        MixAll.string2File(delayOffset, fileName);
                    } catch (IOException e) {
                        //持久化出现异常打印日志
                        log.error("Persist file Exception, {}", fileName, e);
                    }
                }
                //打印日志
                log.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                //同步延迟偏移量出现异常打印日志
                log.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 同步订阅组配置
     */
    private void syncSubscriptionGroupConfig() {
        //主broker地址
        String masterAddrBak = this.masterAddr;
        //主broker地址不等于自身
        if (masterAddrBak != null  && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                SubscriptionGroupWrapper subscriptionWrapper =
                    this.brokerController.getBrokerOuterAPI()
                        .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                    .equals(subscriptionWrapper.getDataVersion())) {
                    SubscriptionGroupManager subscriptionGroupManager =
                        this.brokerController.getSubscriptionGroupManager();
                    subscriptionGroupManager.getDataVersion().assignNewOne(
                        subscriptionWrapper.getDataVersion());
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(
                        subscriptionWrapper.getSubscriptionGroupTable());
                    subscriptionGroupManager.persist();
                    log.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }
}
