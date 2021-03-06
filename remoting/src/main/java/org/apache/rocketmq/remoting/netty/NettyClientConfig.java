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
package org.apache.rocketmq.remoting.netty;

/**
 * netty客户端配置
 */
public class NettyClientConfig {
    /**
     * 客户端工作线程数
     */
    private int clientWorkerThreads = 4;
    /**
     * 客户端回调执行器线程数
     */
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    /**
     * 客户端单向信号量值
     */
    private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;
    /**
     * 客户端异步信号量值
     */
    private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;
    /**
     * 连接超时时间3s
     */
    private int connectTimeoutMillis = 3000;
    /**
     * 通道不活跃的时间间隔
     */
    private long channelNotActiveInterval = 1000 * 60;

    /**
     * 在指定的时间段内未执行读或写操作时，将触发IdleStateEvent。指定{@code 0}以禁用
     */
    private int clientChannelMaxIdleTimeSeconds = 120;

    /**
     * 客户端Socket发送的缓冲区大小
     */
    private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    /**
     * 客户端Socket接收的缓冲区大小
     */
    private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    /**
     * 客户端分配ByteBuf池开关
     */
    private boolean clientPooledByteBufAllocatorEnable = false;
    /**
     * 如果超时客户端关闭socket开关
     */
    private boolean clientCloseSocketIfTimeout = false;

    /**
     * TLS开关
     */
    private boolean useTLS;

    public boolean isClientCloseSocketIfTimeout() {
        return clientCloseSocketIfTimeout;
    }

    public void setClientCloseSocketIfTimeout(final boolean clientCloseSocketIfTimeout) {
        this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
    }

    public int getClientWorkerThreads() {
        return clientWorkerThreads;
    }

    public void setClientWorkerThreads(int clientWorkerThreads) {
        this.clientWorkerThreads = clientWorkerThreads;
    }

    public int getClientOnewaySemaphoreValue() {
        return clientOnewaySemaphoreValue;
    }

    public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue) {
        this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public long getChannelNotActiveInterval() {
        return channelNotActiveInterval;
    }

    public void setChannelNotActiveInterval(long channelNotActiveInterval) {
        this.channelNotActiveInterval = channelNotActiveInterval;
    }

    public int getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }

    public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue) {
        this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
    }

    public int getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }

    public void setClientChannelMaxIdleTimeSeconds(int clientChannelMaxIdleTimeSeconds) {
        this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
    }

    public int getClientSocketSndBufSize() {
        return clientSocketSndBufSize;
    }

    public void setClientSocketSndBufSize(int clientSocketSndBufSize) {
        this.clientSocketSndBufSize = clientSocketSndBufSize;
    }

    public int getClientSocketRcvBufSize() {
        return clientSocketRcvBufSize;
    }

    public void setClientSocketRcvBufSize(int clientSocketRcvBufSize) {
        this.clientSocketRcvBufSize = clientSocketRcvBufSize;
    }

    public boolean isClientPooledByteBufAllocatorEnable() {
        return clientPooledByteBufAllocatorEnable;
    }

    public void setClientPooledByteBufAllocatorEnable(boolean clientPooledByteBufAllocatorEnable) {
        this.clientPooledByteBufAllocatorEnable = clientPooledByteBufAllocatorEnable;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }
}
