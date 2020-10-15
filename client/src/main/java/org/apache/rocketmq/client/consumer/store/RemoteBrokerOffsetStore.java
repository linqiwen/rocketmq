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
package org.apache.rocketmq.client.consumer.store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 远程存储实现
 * <p>
 *     DefaultMQPushConsumer的CLUSTERING模式，由Broker端存储和控制Offset的值，使用RemoteBrokerOffsetStore
 * </p>
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    /**
     * 消费者组名
     */
    private final String groupName;
    /**
     * 消费者对每个消息队列的消费偏移量，互不影响
     * key:消息队列，value:偏移量
     */
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }

    @Override
    public void load() {
    }

    /**
     * 更新消息队列的偏移量
     *
     * @param mq           需更新偏移量的消息队列
     * @param offset       要新增或者设置的偏移量
     * @param increaseOnly true原有的偏移量的基础加上传入offset偏移量
     */
    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            //从内存中获取消息队列消费的偏移量
            AtomicLong offsetOld = this.offsetTable.get(mq);
            //此队列的消息偏移量不存在
            if (null == offsetOld) {
                //将传入进来的消息队列对应的消息偏移量加入内存中
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            //此队列的消息偏移量存在true原有消息队列的偏移量的基础加上传入offset偏移量
            if (null != offsetOld) {
                if (increaseOnly) {
                    //increaseOnly为true在原有偏移量的基础上加上传入进来的偏移量
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    //如果increaseOnly为false，将原有偏移量设置为传入进来的偏移量
                    offsetOld.set(offset);
                }
            }
        }
    }

    /**
     * 读取消息队列的偏移量
     *
     * @param mq   消息队列
     * @param type 读消息队列偏移量的数据源类型
     * @return 该消息队列的偏移量，读取不到返回-1
     */
    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                //先从内存读取，内存读取不到从broker读取
                case MEMORY_FIRST_THEN_STORE:
                //从内存读取
                case READ_FROM_MEMORY: {
                    //从内存中读取此消息队列的偏移量
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        //从内存中读取到此消息队列的偏移量，返回
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        //判断传入的读取类型是否只从内存中读取，如果是，读取不到直接返回-1
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    try {
                        //从broker获取消费者的消费消息队列的偏移量
                        long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                        AtomicLong offset = new AtomicLong(brokerOffset);
                        //更新此消息队列的偏移量
                        this.updateOffset(mq, offset.get(), false);
                        return brokerOffset;
                    }
                    // No offset in broker
                    catch (MQBrokerException e) {
                        return -1;
                    }
                    //Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        return -2;
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        final HashSet<MessageQueue> unusedMQ = new HashSet<MessageQueue>();
        if (!mqs.isEmpty()) {
            for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                AtomicLong offset = entry.getValue();
                if (offset != null) {
                    if (mqs.contains(mq)) {
                        try {
                            this.updateConsumeOffsetToBroker(mq, offset.get());
                            log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                                this.groupName,
                                this.mQClientFactory.getClientId(),
                                mq,
                                offset.get());
                        } catch (Exception e) {
                            log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                        }
                    } else {
                        unusedMQ.add(mq);
                    }
                }
            }
        }

        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    /**
     * 持久化偏移量，偏移量发送给broker进行存储
     *
     * @param mq 消息队列
     */
    @Override
    public void persist(MessageQueue mq) {
        //获取消息队列的消费偏移量
        AtomicLong offset = this.offsetTable.get(mq);
        //消息队列的消费偏移量不为空
        if (offset != null) {
            try {
                //将消息队列的消费偏移量更新到broker
                this.updateConsumeOffsetToBroker(mq, offset.get());
                //打印日志
                log.info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                    this.groupName,
                    this.mQClientFactory.getClientId(),
                    mq,
                    offset.get());
            } catch (Exception e) {
                //出现异常打印日志
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }

    /**
     * 将消息队列的消费偏移量从内存中移除
     *
     * @param mq 消息队列
     */
    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            //从内存中移除消息队列的消费偏移量
            this.offsetTable.remove(mq);
            //打印日志
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
                offsetTable.size());
        }
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());
        }
        return cloneOffsetTable;
    }

    /**
     * 用一种方式更新用户偏移量，一旦主节点关闭，更新到从属节点，这里需要进行优化
     *
     * @param mq 消息队列
     * @param offset 消费消息队列的消费偏移量
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        //将消息队列的消费偏移量更新到broker
        updateConsumeOffsetToBroker(mq, offset, true);
    }

    /**
     * 同步更新用户偏移量，一旦主节点关闭，更新到从属节点，这里需要进行优化
     *
     * @param mq 消息队列
     * @param offset 消费消息队列的偏移量
     * @param isOneway 是否单向发送
     */
    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        //根据brokerName查询存储消息队列的broker信息
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            //从内存中未找到broker信息，从nameServer获取新的主题路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            //再次根据brokerName查询存储消息队列的broker信息
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        //如果查询到队列的broker信息
        if (findBrokerResult != null) {
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            //设置主题
            requestHeader.setTopic(mq.getTopic());
            //设置消费者组
            requestHeader.setConsumerGroup(this.groupName);
            //设置队列id
            requestHeader.setQueueId(mq.getQueueId());
            //设置commitLog偏移量
            requestHeader.setCommitOffset(offset);

            if (isOneway) {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            } else {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            }
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }

    /**
     * 从broker获取传入进来的消息队列在此消费者的偏移量
     *
     * @param mq 消息队列
     * @return 消费者消费此消息队列的偏移量
     */
    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {
        //从当前消费者中本地缓存的broker中获取消息队列中发broker信息
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            //当前消费者中本地缓存的broker中不存在消息队列的broker信息，从nameServer中此topic的路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            //再次从当前消费者中本地缓存的broker中获取消息队列中发broker信息
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            //设置主题
            requestHeader.setTopic(mq.getTopic());
            //设置消费者组
            requestHeader.setConsumerGroup(this.groupName);
            //设置队列id
            requestHeader.setQueueId(mq.getQueueId());

            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }
}
