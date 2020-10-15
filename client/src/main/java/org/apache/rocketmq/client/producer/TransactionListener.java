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
package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事务监听
 * <p>
 *     事务生产者发送半消息成功，调用executeLocalTransaction执行本地事务
 *     当broker没有收到事务生产者的提交或回滚指令，一定时间调用checkLocalTransaction回查本地事务状态
 * </p>
 */
public interface TransactionListener {
    /**
     * 当发送事务半消息成功, 将调用此方法来执行本地事务
     *
     * @param msg 半消息，对消费者不可见
     * @param arg 自定义业务参数
     * @return 本地事务状态
     */
    LocalTransactionState executeLocalTransaction(final Message msg, final Object arg);

    /**
     * 当事务生产者没有响应半消息. broker将发送检查消息来检查事务状态，并调用此方法来获取本地事务状态.
     *
     * @param msg 检查消息
     * @return 本地事务状态
     */
    LocalTransactionState checkLocalTransaction(final MessageExt msg);
}