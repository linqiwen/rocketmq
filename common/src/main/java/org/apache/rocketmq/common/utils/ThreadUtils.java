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

package org.apache.rocketmq.common.utils;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 线程工具类
 */
public final class ThreadUtils {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TOOLS_LOGGER_NAME);

    /**
     * 创建一个corePoolSize<=线程数线程池<=maximumPoolSize,队列是workQueue，workQueue如果为无界maximumPoolSize参数不起作用
     * <p>
     *     线程池的核心执行流程
     *     1.提交任务一到线程池，如果线程池中的线程数小于核心线程数，创建一个新的线程进行处理任务，不管线程池中有无空闲线程
     *     2.提交任务二到线程池，如果线程池中的线程数大于等于核心线程数，将其任务加入到队列中
     *     3.提交任务三到线程池，如果线程池中的线程数已经大于等于核心线程数，并且队列已满，线程池中的线程数小于最大线程数，创建一个新线程处理任务
     *     4.线程池队列已满，并且线程数已经等于最大线程数，执行拒绝策略
     *     5.如果线程池中的核心线程数设置成0，在第一步中也会创建一个核心线程
     *     6.如果线程池中队列是无边界队列，最大核心线程数参数无效
     * </p>
     *
     * @param corePoolSize 线程池的核心线程数
     * @param maximumPoolSize 最大核心线程数
     * @param keepAliveTime 这是非核心空闲线程终止之前将等待新的任务最长时间，如果线程池设置allowsCoreThreadTimeOut()，也会终止
     * @param unit keepAliveTime的时间单位
     * @param isDaemon 是否后台线程，即是否会阻止jvm暂停
     * @param processName 线程池中的线程部分名称
     * @param workQueue 任务队列
     * @return 执行器
     * @see ThreadPoolExecutor
     */
    public static ExecutorService newThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        TimeUnit unit, BlockingQueue<Runnable> workQueue, String processName, boolean isDaemon) {
        return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, newThreadFactory(processName, isDaemon));
    }

    /**
     * 创建单个线程的线程池
     *
     * @param processName 线程池中的线程部分名称
     * @param isDaemon 是否后台线程，即是否会阻止jvm暂停
     * @return 执行器
     */
    public static ExecutorService newSingleThreadExecutor(String processName, boolean isDaemon) {
        return Executors.newSingleThreadExecutor(newThreadFactory(processName, isDaemon));
    }

    /**
     * 创建单个线程的调度线程池
     *
     * @param processName 线程池中的线程部分名称
     * @param isDaemon 是否后台线程，即是否会阻止jvm暂停
     * @return 调度执行器
     * @see ScheduledThreadPoolExecutor
     */
    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String processName, boolean isDaemon) {
        return Executors.newSingleThreadScheduledExecutor(newThreadFactory(processName, isDaemon));
    }

    /**
     * 创建固定线程数的调度线程池
     *
     * @param nThreads 线程数
     * @param processName 线程池中的线程部分名称
     * @param isDaemon 是否后台线程，即是否会阻止jvm暂停
     * @return 调度执行器
     */
    public static ScheduledExecutorService newFixedThreadScheduledPool(int nThreads, String processName,
        boolean isDaemon) {
        return Executors.newScheduledThreadPool(nThreads, newThreadFactory(processName, isDaemon));
    }

    /**
     * 创建一个线程工厂
     *
     * @param processName 线程部分名称
     * @param isDaemon 是否守护线程，即是否会阻止jvm暂停
     * @return 线程工厂
     */
    public static ThreadFactory newThreadFactory(String processName, boolean isDaemon) {
        return newGenericThreadFactory("Remoting-" + processName, isDaemon);
    }

    /**
     * 创建一个只创建非守护线程的线程工厂
     *
     * @param processName 线程部分名称
     */
    public static ThreadFactory newGenericThreadFactory(String processName) {
        return newGenericThreadFactory(processName, false);
    }

    public static ThreadFactory newGenericThreadFactory(String processName, int threads) {
        return newGenericThreadFactory(processName, threads, false);
    }

    public static ThreadFactory newGenericThreadFactory(final String processName, final boolean isDaemon) {
        return new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, String.format("%s_%d", processName, this.threadIndex.incrementAndGet()));
                thread.setDaemon(isDaemon);
                return thread;
            }
        };
    }

    public static ThreadFactory newGenericThreadFactory(final String processName, final int threads,
        final boolean isDaemon) {
        return new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, String.format("%s_%d_%d", processName, threads, this.threadIndex.incrementAndGet()));
                thread.setDaemon(isDaemon);
                return thread;
            }
        };
    }

    /**
     * 创建新线程
     *
     * @param name The name of the thread
     * @param runnable The work for the thread to do
     * @param daemon Should the thread block JVM stop?
     * @return The unstarted thread
     */
    public static Thread newThread(String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Uncaught exception in thread '" + t.getName() + "':", e);
            }
        });
        return thread;
    }

    /**
     * 使用线程的isAlive和join关闭线程，优雅的关闭线程.
     *
     * @param t 关闭线程
     */
    public static void shutdownGracefully(final Thread t) {
        shutdownGracefully(t, 0);
    }

    /**
     * 使用线程的isAlive和join关闭线程，优雅的关闭线程.
     *
     * @param millis 等待时间，如果0线程将永远等待.
     * @param t 停止线程
     */
    public static void shutdownGracefully(final Thread t, final long millis) {
        if (t == null)
            return;
        while (t.isAlive()) {
            try {
                t.interrupt();
                t.join(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 优雅的暂停{@link ExecutorService}执行器.
     *
     * @param executor executor 执行器
     * @param timeout timeout 超时时间
     * @param timeUnit timeUnit 时间单位
     */
    public static void shutdownGracefully(ExecutorService executor, long timeout, TimeUnit timeUnit) {
        // 阻止新的任务提交到线程池中.
        executor.shutdown();
        try {
            // Wait a while for existing tasks to terminate.
            if (!executor.awaitTermination(timeout, timeUnit)) {
                executor.shutdownNow();
                // Wait a while for tasks to respond to being cancelled.
                if (!executor.awaitTermination(timeout, timeUnit)) {
                    log.warn(String.format("%s didn't terminate!", executor));
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted.
            executor.shutdownNow();
            // Preserve interrupt status.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 私有的构造方法，防止构造这个类
     * A constructor to stop this class being constructed.
     */
    private ThreadUtils() {
        // Unused

    }
}
