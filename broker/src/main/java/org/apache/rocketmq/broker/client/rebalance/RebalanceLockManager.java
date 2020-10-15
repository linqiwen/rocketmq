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
package org.apache.rocketmq.broker.client.rebalance;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageQueue;

public class RebalanceLockManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    /**
     * 平衡锁最大生存时间
     */
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    /**
     * key:消费组，value<key:消息队列，锁入口>
     */
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
        new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);

    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info("tryLock, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                            group,
                            clientId,
                            mq);
                    }

                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    String oldClientId = lockEntry.getClientId();

                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                            "tryLock, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);
                        return true;
                    }

                    log.warn(
                        "tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                        group,
                        oldClientId,
                        clientId,
                        mq);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        } else {

        }

        return true;
    }

    /**
     * 消费组中的客户端id是否对mq进行加锁
     *
     * @param group 消费组
     * @param mq 消息队列
     * @param clientId 客户端id
     * @return {@code true}消费组中的客户端id是否对mq进行加锁
     */
    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        //获取消费组中所有客户端加锁的消息队列
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            //根据消息队列获取加锁的信息
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                //判断传入进来的客户端是否对队列进行加锁
                boolean locked = lockEntry.isLocked(clientId);
                if (locked) {
                    //更新时间
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }

    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs,
        final String clientId) {
        Set<MessageQueue> lockedMqs = new HashSet<MessageQueue>(mqs.size());
        Set<MessageQueue> notLockedMqs = new HashSet<MessageQueue>(mqs.size());

        //遍历传入进来的所有消息队列
        for (MessageQueue mq : mqs) {
            //判断客户端id对哪些队列已经进行加锁，哪些还未进行加锁
            if (this.isLocked(group, mq, clientId)) {
                //将队列加入到加锁队列中
                lockedMqs.add(mq);
            } else {
                //将队列加入未加锁队列中
                notLockedMqs.add(mq);
            }
        }

        //未加锁的队列不为空
        if (!notLockedMqs.isEmpty()) {
            try {
                //获取独占的中断锁
                this.lock.lockInterruptibly();
                try {
                    //获取消费组中所有客户端加锁的消息队列
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        //消费组中不存在客户端加锁的消息队列，新创建一个
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    for (MessageQueue mq : notLockedMqs) {
                        //根据消息队列获取加锁的信息
                        LockEntry lockEntry = groupValue.get(mq);
                        //不存在一个消息队列的加锁信息
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            //将新创建的LockEntry加入到groupValue中
                            groupValue.put(mq, lockEntry);
                            //打印日志
                            log.info(
                                "tryLockBatch, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                                group,
                                clientId,
                                mq);
                        }

                        //lockEntry已经被clientId锁定
                        if (lockEntry.isLocked(clientId)) {
                            //更新时间
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            //加入到已锁mq队列中
                            lockedMqs.add(mq);
                            continue;
                        }

                        //否则获取老的客户端id
                        String oldClientId = lockEntry.getClientId();

                        //老的客户端id对队列进行加锁超时
                        if (lockEntry.isExpired()) {
                            //新客户端对消息队列进行加锁
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                "tryLockBatch, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                                group,
                                oldClientId,
                                clientId,
                                mq);
                            lockedMqs.add(mq);
                            continue;
                        }

                        //打印日志
                        log.warn(
                            "tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);
                    }
                } finally {
                    //释放锁
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                //出现异常打印日志
                log.error("putMessage exception", e);
            }
        }

        return lockedMqs;
    }

    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            //加独占可中断锁
            this.lock.lockInterruptibly();
            try {
                //从锁消息队列列表中获取消息队列和锁的映射信息
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    //遍历消息队列
                    for (MessageQueue mq : mqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null != lockEntry) {
                            //如果锁消息队列的消费者和传入进来的客户端id相同
                            if (lockEntry.getClientId().equals(clientId)) {
                                //客户端id相等移除
                                groupValue.remove(mq);
                                //打印日志
                                log.info("unlockBatch, Group: {} {} {}",
                                    group,
                                    mq,
                                    clientId);
                            } else {
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}",
                                    lockEntry.getClientId(),
                                    group,
                                    mq,
                                    clientId);
                            }
                        } else {
                            //打印日志
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}",
                                group,
                                mq,
                                clientId);
                        }
                    }
                } else {
                    //打印日志
                    log.warn("unlockBatch, group not exist, Group: {} {}",
                        group,
                        clientId);
                }
            } finally {
                //释放锁
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            //出现异常打印日志
            log.error("putMessage exception", e);
        }
    }

    static class LockEntry {
        /**
         * 客户端id
         */
        private String clientId;
        /**
         * 最近的更新时间
         */
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        /**
         * 判断客户端是否被锁定
         */
        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            //客户端id相等并且未过期
            return eq && !this.isExpired();
        }

        /**
         * 是否过期
         *
         * @return {@code true}过期
         */
        public boolean isExpired() {
            boolean expired =
                (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
