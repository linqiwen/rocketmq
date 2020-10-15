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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
/**
 * 服务线程
 */
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 关闭任务时，等待执行线程关闭的等待时间
     */
    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 执行当前任务线程
     */
    private Thread thread;
    /**
     * 等待点
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    /**
     * 是否通知等待线程
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    /**
     * 是否停止
     */
    protected volatile boolean stopped = false;
    /**
     * 是否后台线程
     */
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    //标识当前任务是否开始执行
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    /**
     * 服务名
     */
    public abstract String getServiceName();

    /**
     * 开始执行当前任务
     */
    public void start() {
        //打印日志
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        //如果当前服务已经启动过直接返回
        if (!started.compareAndSet(false, true)) {
            return;
        }
        //停止标识置为false
        stopped = false;
        //创建执行当前服务任务的线程
        this.thread = new Thread(this, getServiceName());
        //设置当前线程是否后台线程
        this.thread.setDaemon(isDaemon);
        //启动线程
        this.thread.start();
    }

    /**
     * 关闭服务
     */
    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 关闭服务
     *
     * @param interrupt 是否需要中断
     */
    public void shutdown(final boolean interrupt) {
        //打印日志
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        //服务启动标识置为false，如果服务已经被关闭直接返回
        if (!started.compareAndSet(true, false)) {
            return;
        }
        //服务停止标识置为true
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        //通知其他等待此服务执行完的线程
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                //如果中断标识为true，中断当前线程
                this.thread.interrupt();
            }

            //开始时间
            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                //如果执行线程不是后台线程，关闭线程等待一定时间
                this.thread.join(this.getJointime());
            }
            //如果不是后台线程，即join的时间
            long eclipseTime = System.currentTimeMillis() - beginTime;
            //打印日志
            log.info("join thread " + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    /**
     * 停止服务，目前不建议使用
     */
    @Deprecated
    public void stop() {
        this.stop(false);
    }

    /**
     * 停止服务，目前不建议使用，方法里不会改变start标识
     *
     * @param interrupt 是否需要中断
     */
    @Deprecated
    public void stop(final boolean interrupt) {
        //如果服务已经停止直接返回
        if (!started.get()) {
            return;
        }
        //服务停止标识置为true
        this.stopped = true;
        //打印日志
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);
        //通知其他等待此服务执行完的线程
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            //如果中断标识为true，中断当前线程
            this.thread.interrupt();
        }
    }

    /**
     * 将stopped设置为true
     */
    public void makeStop() {
        if (!started.get()) {
            return;
        }
        //服务停止标识置为true
        this.stopped = true;
        //打印日志
        log.info("makestop thread " + this.getServiceName());
    }

    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            //等待结束后执行
            this.onWaitEnd();
            return;
        }

        //重置等待点
        waitPoint.reset();

        try {
            //等待一定的时间执行
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            //等待结束后执行
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
