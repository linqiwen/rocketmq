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
package org.apache.rocketmq.broker.transaction;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 事务消息检查服务
 */
public class TransactionalMessageCheckService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    /**
     * broker控制器
     */
    private BrokerController brokerController;

    public TransactionalMessageCheckService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public String getServiceName() {
        return TransactionalMessageCheckService.class.getSimpleName();
    }

    @Override
    public void run() {
        //打印事务消息检查服务开始运行
        log.info("Start transaction check service thread!");
        //获取事务消息回查间隔
        long checkInterval = brokerController.getBrokerConfig().getTransactionCheckInterval();
        while (!this.isStopped()) {
            //服务未关闭，等待一定时间执行
            this.waitForRunning(checkInterval);
        }
        //打印事务消息检查服务运行结束
        log.info("End transaction check service thread!");
    }

    @Override
    protected void onWaitEnd() {
        //获取事务消息被检查的超时时间
        long timeout = brokerController.getBrokerConfig().getTransactionTimeOut();
        //获取检查消息的最大次数，如果超过此值，此消息将被丢弃
        int checkMax = brokerController.getBrokerConfig().getTransactionCheckMax();
        //事务消息被检查的开始时间
        long begin = System.currentTimeMillis();
        //打印事务消息被检查的开始时间
        log.info("Begin to check prepare message, begin time:{}", begin);
        //开始事务消息检查
        this.brokerController.getTransactionalMessageService().check(timeout, checkMax, this.brokerController.getTransactionalMessageCheckListener());
        //打印日志，耗费的时间
        log.info("End to check prepare message, consumed time:{}", System.currentTimeMillis() - begin);
    }

}
