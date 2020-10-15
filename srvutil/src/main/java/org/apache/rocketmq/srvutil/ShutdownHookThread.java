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

package org.apache.rocketmq.srvutil;

import org.apache.rocketmq.logging.InternalLogger;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ShutdownHookThread} 是filtersrv和namesrv的标准钩子模块。通过{@link Callable}接口，这个钩子可以在任何地方定制操作
 */
public class ShutdownHookThread extends Thread {
    /**
     * 钩子线程是否已关闭
     */
    private volatile boolean hasShutdown = false;
    /**
     * 关闭次数
     */
    private AtomicInteger shutdownTimes = new AtomicInteger(0);
    /**
     * 日志
     */
    private final InternalLogger log;
    /**
     * 回调方法
     */
    private final Callable callback;

    /**
     * 创建标准的钩子线程, 使用{@link Callable}接口进行回调
     *
     * @param log 日志实例在钩子线程中使用
     * @param callback 回调函数
     */
    public ShutdownHookThread(InternalLogger log, Callable callback) {
        super("ShutdownHook");
        this.log = log;
        this.callback = callback;
    }

    /**
     * 线程运行方法.
     * jvm关闭时调用.
     * 1. 计算调用次数.
     * 2. 执行{@link ShutdownHookThread#callback}, 并计时.
     */
    @Override
    public void run() {
        synchronized (this) {
            //打印日志
            log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet() + " times.");
            //如果线程没有被关闭
            if (!this.hasShutdown) {
                //钩子线程是否已关闭标识置为true
                this.hasShutdown = true;
                //回调方法的开始执行时间
                long beginTime = System.currentTimeMillis();
                try {
                    //执行回调方法
                    this.callback.call();
                } catch (Exception e) {
                    //出现异常打印日志
                    log.error("shutdown hook callback invoked failure.", e);
                }
                //执行回调方法耗费的时间
                long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                //打印日志
                log.info("shutdown hook done, consuming time total(ms): " + consumingTimeTotal);
            }
        }
    }
}
