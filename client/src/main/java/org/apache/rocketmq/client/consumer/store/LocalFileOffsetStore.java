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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消费者消费消息队列偏移量的本地存储实现
 * <p>
 *     DefaultMQPushConsumer为BROADCASTING模式，各个Consumer没有互相干扰，使用LocalFileOffsetStore，把Offset存储在Consumer本地
 * </p>
 */
public class LocalFileOffsetStore implements OffsetStore {
    /**
     * 队列偏移量持久化的磁盘地址
     */
    public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty(
            "rocketmq.client.localOffsetStoreDir",
            System.getProperty("user.home") + File.separator + ".rocketmq_offsets");
    private final static InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    /**
     * 消费者组名
     */
    private final String groupName;
    /**
     * 消费者消费消息队列的偏移量存储位置
     */
    private final String storePath;
    /**
     * 消费者对每个消息队列的消费偏移量，互不影响
     * key:消息队列，value:偏移量
     */
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public LocalFileOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
        this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator +
                this.mQClientFactory.getClientId() + File.separator +
                this.groupName + File.separator +
                "offsets.json";
    }

    /**
     * 从磁盘文件中加载当前消费者对各个消息队列的消费偏移量到offsetTable中
     */
    @Override
    public void load() throws MQClientException {
        //从本地磁盘文件中读取消息队列的消费偏移量
        OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
        //消息队列的消费偏移量数据存在
        if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
            //将本地磁盘文件中读取消息队列的消费偏移量加入到内存中
            offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());

            //打印日志
            for (MessageQueue mq : offsetSerializeWrapper.getOffsetTable().keySet()) {
                AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                log.info("load consumer's offset, {} {} {}",
                        this.groupName,
                        mq,
                        offset.get());
            }
        }
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
                    //increaseOnly为true使用cas更新为传入进来的偏移量
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
                //先从内存读取，内存读取不到从磁盘读取
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
                //从磁盘中读取
                case READ_FROM_STORE: {
                    OffsetSerializeWrapper offsetSerializeWrapper;
                    try {
                        //从本地磁盘文件中读取消息队列的消费偏移量
                        offsetSerializeWrapper = this.readLocalOffset();
                    } catch (MQClientException e) {
                        return -1;
                    }
                    //如果本地磁盘文件中读取消息队列的消费偏移量存在
                    if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                        //获取此消息队列的偏移量
                        AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                        //磁盘中消息队列的消费偏移量数据存在
                        if (offset != null) {
                            //更新内存中此消息队列的偏移量
                            this.updateOffset(mq, offset.get(), false);
                            //返回读取到的消息队列的消息偏移量
                            return offset.get();
                        }
                    }
                }
                //默认退出，返回-1
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

        OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
        //持久化内存的传入进来的所有消息队列偏移量
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            //如果内存中的消息队列在传入进来的消息队列中
            if (mqs.contains(entry.getKey())) {
                AtomicLong offset = entry.getValue();
                offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }

        //将offsetSerializeWrapper序列化为字符串
        String jsonString = offsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            try {
                //将字符串存储到本地文件中
                MixAll.string2File(jsonString, this.storePath);
            } catch (IOException e) {
                log.error("persistAll consumer offset Exception, " + this.storePath, e);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
    }

    @Override
    public void removeOffset(MessageQueue mq) {

    }

    @Override
    public void updateConsumeOffsetToBroker(final MessageQueue mq, final long offset, final boolean isOneway)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

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
     * 获取消费者消费消息队列的本地偏移量，本地偏移量在广播模式使用
     */
    private OffsetSerializeWrapper readLocalOffset() throws MQClientException {
        String content = null;
        try {
            //获取文件内容
            content = MixAll.file2String(this.storePath);
        } catch (IOException e) {
            log.warn("Load local offset store file exception", e);
        }
        //消费者消费消息队列的偏移量存储位置文件内容为空
        if (null == content || content.length() == 0) {
            //从备份文件读取
            return this.readLocalOffsetBak();
        } else {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                //将文件内容反序列化为消息队列消费偏移量序列化包装类
                offsetSerializeWrapper =
                        OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception, and try to correct", e);
                //如果读取消费者消费消息队列的偏移量存储位置文件内容异常，读取从备份文件读取
                return this.readLocalOffsetBak();
            }

            return offsetSerializeWrapper;
        }
    }

    private OffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath + ".bak");
        } catch (IOException e) {
            log.warn("Load local offset store bak file exception", e);
        }
        if (content != null && content.length() > 0) {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                        OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception", e);
                throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low"
                        + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION),
                        e);
            }
            return offsetSerializeWrapper;
        }

        return null;
    }
}
