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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.common.MixAll;

import java.nio.charset.Charset;

/**
 * 事务消息工具类
 */
public class TransactionalMessageUtil {
    public static final String REMOVETAG = "d";
    /**
     * 编码
     */
    public static Charset charset = Charset.forName("utf-8");

    /**
     * 获取已被处理半消息主题
     * <p>
     *     在半消息被commit或者rollback处理后，会存储到Topic为RMQ_SYS_TRANS_OP_HALF_TOPIC的队列中，标识半消息已被处理
     * </p>
     *
     */
    public static String buildOpTopic() {
        return MixAll.RMQ_SYS_TRANS_OP_HALF_TOPIC;
    }

    /**
     * 获取半消息存放主题
     * <p>
     *     半消息在被投递到Mq服务器后，会存储于Topic为RMQ_SYS_TRANS_HALF_TOPIC的消费队列中
     * </p>
     */
    public static String buildHalfTopic() {
        return MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
    }

    /**
     * 事务消息的消费者组
     */
    public static String buildConsumerGroup() {
        return MixAll.CID_SYS_RMQ_TRANS;
    }

}
