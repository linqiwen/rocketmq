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
package org.apache.rocketmq.remoting;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 远程客户端
 */
public interface RemotingClient extends RemotingService {

    /**
     * 更新nameServer地址列表
     */
    void updateNameServerAddressList(final List<String> addrs);

    /**
     * 获取nameServer地址列表
     */
    List<String> getNameServerAddressList();

    /**
     * 同步发送消息
     *
     * @param addr 远程地址
     * @param request 发送的请求
     * @param timeoutMillis 执行的超时时间
     * @return 远程回复的命令
     * @throws InterruptedException 等待远程响应，有其他线程中断当前等待线程
     * @throws RemotingConnectException 连接远程异常
     * @throws RemotingSendRequestException 远程发送请求异常
     * @throws RemotingTimeoutException 远程服务响应超时异常
     * @throws 远程发送请求异常
     */
    RemotingCommand invokeSync(final String addr, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步发送消息
     *
     * @param addr 远程地址
     * @param request 发送的请求
     * @param timeoutMillis 执行的超时时间
     * @param invokeCallback 远程服务响应回来的回调方法
     * @throws InterruptedException 等待远程响应，有其他线程中断当前等待线程，异步中可以忽略
     * @throws RemotingConnectException 连接远程异常
     * @throws RemotingTooMuchRequestException 远程太多请求异常
     * @throws RemotingTimeoutException 远程服务响应超时异常
     * @throws RemotingSendRequestException 远程发送请求异常
     */
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 单向发送消息
     *
     * @param addr 远程地址
     * @param request 发送的请求
     * @param timeoutMillis 执行的超时时间
     * @throws InterruptedException 等待远程响应，有其他线程中断当前等待线程
     * @throws RemotingConnectException 连接远程异常
     * @throws RemotingTooMuchRequestException 远程太多请求异常
     * @throws RemotingTimeoutException 远程服务响应超时异常
     * @throws RemotingSendRequestException 远程发送请求异常
     */
    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 注册请求处理器
     *
     * @param requestCode 请求码
     * @param processor 请求处理器
     * @param executor 线程池
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    /**
     * 设置回调执行器
     */
    void setCallbackExecutor(final ExecutorService callbackExecutor);

    /**
     * 获取回调执行器
     */
    ExecutorService getCallbackExecutor();

    /**
     * 判断这个地址通道是否可写
     */
    boolean isChannelWritable(final String addr);
}
