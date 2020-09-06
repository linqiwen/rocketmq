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

import io.netty.channel.Channel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 响应future
 */
public class ResponseFuture {
    /**
     * 请求id
     */
    private final int opaque;
    /**
     * 通道
     */
    private final Channel processChannel;
    /**
     * 超时时间
     */
    private final long timeoutMillis;
    /**
     * 回调方法
     */
    private final InvokeCallback invokeCallback;
    /**
     * 开始时间
     */
    private final long beginTimestamp = System.currentTimeMillis();
    /**
     * 使用countDownLatch等待服务响应唤醒
     */
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 只释放一次的信号量
     */
    private final SemaphoreReleaseOnlyOnce once;

    /**
     * 只执行一次回调
     */
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    /**
     * 响应的命令
     */
    private volatile RemotingCommand responseCommand;
    /**
     * 发送请求是否ok
     */
    private volatile boolean sendRequestOK = true;
    /**
     * 异常
     */
    private volatile Throwable cause;

    public ResponseFuture(Channel channel, int opaque, long timeoutMillis, InvokeCallback invokeCallback,
        SemaphoreReleaseOnlyOnce once) {
        this.opaque = opaque;
        this.processChannel = channel;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    /**
     * 执行回调方法
     */
    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            //回调方法只执行一次
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }

    /**
     * 释放信号量
     */
    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    /**
     * 判断是否超时
     */
    public boolean isTimeout() {
        //当前时间和开始时间的时间差
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        //如果时间差大于超时时间，超时
        return diff > this.timeoutMillis;
    }

    /**
     * 等待远程响应
     *
     * @param timeoutMillis 等待的超时时间
     * @return 远程命令
     */
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        //使用countDownLatch等待，直到有线程调用countDown
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    /**
     * 设置远程响应命令，并唤醒等待线程
     *
     * @param responseCommand 响应命令
     */
    public void putResponse(final RemotingCommand responseCommand) {
        //设置响应命令
        this.responseCommand = responseCommand;
        //唤醒等待线程
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public int getOpaque() {
        return opaque;
    }

    public Channel getProcessChannel() {
        return processChannel;
    }

    @Override
    public String toString() {
        return "ResponseFuture [responseCommand=" + responseCommand
            + ", sendRequestOK=" + sendRequestOK
            + ", cause=" + cause
            + ", opaque=" + opaque
            + ", processChannel=" + processChannel
            + ", timeoutMillis=" + timeoutMillis
            + ", invokeCallback=" + invokeCallback
            + ", beginTimestamp=" + beginTimestamp
            + ", countDownLatch=" + countDownLatch + "]";
    }
}
