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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

/**
 * netty远程抽象
 */
public abstract class NettyRemotingAbstract {

    /**
     * 远程日志实例.
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * 限制正在进行的单向请求的最大数量的信号量，这可以保护系统内存占用.
     */
    protected final Semaphore semaphoreOneway;

    /**
     * 限制正在进行的异步请求的最大数量的信号量，这可以保护系统内存占用.
     */
    protected final Semaphore semaphoreAsync;

    /**
     * 此映射缓存所有正在进行的请求.
     * key:请求id，value：响应future
     * 使用此map进行同步发送消息等待，响应后被唤醒
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
        new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * 这个容器包含每个请求代码的所有处理器，对于每个传入的请求，
     * 我们可以在这个映射中查找响应处理器来处理请求
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
        new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    /**
     * 将netty事件提供给用户定义的{@link ChannelEventListener}的执行器.
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request code.
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    /**
     * SSL上下文通过它创建{@link SslHandler}
     */
    protected volatile SslContext sslContext;

    /**
     * 自定义RPC钩子集合
     */
    protected List<RPCHook> rpcHooks = new ArrayList<RPCHook>();



    static {
        NettyLogger.initNettyLogger();
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     *
     * @param permitsOneway Number of permits for one-way requests.
     * @param permitsAsync Number of permits for asynchronous requests.
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * 把netty事件交给执行器
     *
     * @param event netty事件实例
     */
    public void putNettyEvent(final NettyEvent event) {
        //将netty事件交给执行器
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * 输入命令处理.
     *
     * <p>
     * <strong>注意:</strong>
     * 传入的远程处理命令可能是
     * <ul>
     * <li>来自远程服务的查询请求;</li>
     * <li>对这个参与者之前发出的请求的响应.</li>
     * </ul>
     * </p>
     *
     * @param ctx 通道处理程序上下文.
     * @param msg 传入远程处理命令.
     * @throws Exception 如果在处理传入命令时出现任何错误.
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            //获取命令的请求类型
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    //处理请求命令
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    //处理响应命令
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 在命令之前执行所有钩子的doBeforeRequest方法
     *
     * @param addr 地址
     * @param request 请求命令
     */
    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook: rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    /**
     * 在命令之后执行所有钩子的doAfterResponse方法
     *
     * @param addr 地址
     * @param request 请求命令
     */
    protected void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook: rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }


    /**
     * 处理远程对等方发出的传入请求命令.
     *
     * @param ctx 通道处理程序上下文.
     * @param cmd 请求命令.
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        //获取请求命令编码的对等处理器和线程池
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        //如果请求命令编码的对等处理器和线程池不存在使用默认的请求处理器
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
        //获取请求id
        final int opaque = cmd.getOpaque();

        if (pair != null) {
            //创建任务
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        //请求处理之前执行钩子方法
                        doBeforeRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        //处理请求
                        final RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                        //请求处理之后执行钩子方法
                        doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);
                        //判读是否是单向请求，如果是单向请求不做任何处理
                        if (!cmd.isOnewayRPC()) {//不是单向请求
                            if (response != null) {
                                //将远端的请求id，返回回去，主要是用来唤醒阻塞的线程或者执行回调方法
                                response.setOpaque(opaque);
                                //标记当前命令是响应命令
                                response.markResponseType();
                                try {
                                    //发送消息
                                    ctx.writeAndFlush(response);
                                } catch (Throwable e) {
                                    //出现异常打印日志
                                    log.error("process request over, but response failed", e);
                                    log.error(cmd.toString());
                                    log.error(response.toString());
                                }
                            } else {
                                //如果响应命令为空不做任何处理
                            }
                        }
                    } catch (Throwable e) {
                        log.error("process request exception", e);
                        log.error(cmd.toString());
                        //如果出现异常，并且不是单向请求，构造系统异常的响应命令
                        if (!cmd.isOnewayRPC()) {
                            //构造响应命令
                            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                                RemotingHelper.exceptionSimpleDesc(e));
                            //将远端的请求id，返回回去，主要是用来唤醒阻塞的线程或者执行回调方法
                            response.setOpaque(opaque);
                            //发送命令
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            //处理器是否拒绝请求，如果系统过于繁忙，处理器会拒绝请求
            if (pair.getObject1().rejectRequest()) {
                //构造系统繁忙命令
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[REJECTREQUEST]system busy, start flow control for a while");
                //将远端的请求id，返回回去，主要是用来唤醒阻塞的线程或者执行回调方法
                response.setOpaque(opaque);
                //发送命令
                ctx.writeAndFlush(response);
                return;
            }

            try {
                //创建请求任务
                final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
                //将请求任务提交到线程池中
                pair.getObject2().submit(requestTask);
            } catch (RejectedExecutionException e) {//如果线程池满（队列满，并且线程池中的线程数大于等于最大线程数）
                //每隔10秒记录下日志
                if ((System.currentTimeMillis() % 10000) == 0) {
                    //打印日志
                    log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                        + ", too many requests and system thread pool busy, RejectedExecutionException "
                        + pair.getObject2().toString()
                        + " request code: " + cmd.getCode());
                }
                //判读是否单向请求
                if (!cmd.isOnewayRPC()) {
                    //如果不是单向请求，响应系统繁忙命令
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                        "[OVERLOAD]system busy, start flow control for a while");
                    //将远端的请求id，返回回去，主要是用来唤醒阻塞的线程或者执行回调方法
                    response.setOpaque(opaque);
                    //发送命令
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            //请求的编码不支持
            String error = " request type " + cmd.getCode() + " not supported";
            //响应系统不支持此请求编码，构造远程命令
            final RemotingCommand response =
                RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            //将远端的请求id，返回回去，主要是用来唤醒阻塞的线程或者执行回调方法
            response.setOpaque(opaque);
            //发送命令
            ctx.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }

    /**
     * 处理远程对等方对以前发出的请求的响应
     *
     * @param ctx 通道处理器上下文
     * @param cmd 响应命令实例
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        //获取请求id
        final int opaque = cmd.getOpaque();
        //根据请求id获取响应future
        final ResponseFuture responseFuture = responseTable.get(opaque);
        //响应future不为空
        if (responseFuture != null) {
            //设置响应命令
            responseFuture.setResponseCommand(cmd);
            //将请求id所对应响应future从responseTable中移除
            responseTable.remove(opaque);

            //响应future回调方法如果不为空
            if (responseFuture.getInvokeCallback() != null) {
                //执行回调方法
                executeInvokeCallback(responseFuture);
            } else {
                //设置远程响应命令，并唤醒等待线程
                responseFuture.putResponse(cmd);
                //释放信号量
                responseFuture.release();
            }
        } else {
            //根据请求id获取响应future为空，超过一定时间没有响应，响应future从responseTable中移除
            log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            log.warn(cmd.toString());
        }
    }

    /**
     * 执行回调,回调执行程序。如果回调执行器为空或者执行器无法执行此任务,直接运行在当前线程
     *
     * @param responseFuture 响应future
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        //运行在当前线程的标识
        boolean runInThisThread = false;
        //回调方法执行器
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null) {
            try {
                //将回调方法当做任务提交到执行器中执行
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            //执行响应future的回调方法
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            //出现异常打印日志
                            log.warn("execute callback in executor exception, and callback throw", e);
                        } finally {
                            //唤醒等待线程
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                //出现异常运行在当前线程
                runInThisThread = true;
                //打印日志
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            //回调方法执行器为空，回调方法运行在当前线程
            runInThisThread = true;
        }

        if (runInThisThread) {
            //如果回调方法运行在当前线程
            try {
                //执行响应future的回调方法
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                //打印日志
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                //释放信号量
                responseFuture.release();
            }
        }
    }



    /**
     * Custom RPC hook.
     * Just be compatible with the previous version, use getRPCHooks instead.
     */
    @Deprecated
    protected RPCHook getRPCHook() {
        if (rpcHooks.size() > 0) {
            return rpcHooks.get(0);
        }
        return null;
    }

    /**
     * Custom RPC hooks.
     *
     * @return RPC hooks if specified; null otherwise.
     */
    public List<RPCHook> getRPCHooks() {
        return rpcHooks;
    }


    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * <p>
     *      定期调用此方法来扫描已超时的请求并使其过期
     * </p>
     */
    public void scanResponseTable() {
        //存放超时的响应future列表
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        //获取响应请求列表的迭代器
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        //获取响应请求列表的迭代器是否有下一个元素
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            //获取到响应Future
            ResponseFuture rep = next.getValue();
            //响应的开始时间+响应的超时时间+1000毫秒如果小于等于当前时间，
            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                //释放信号量
                rep.release();
                //从响应future列表中移除
                it.remove();
                //将超时的响应future加到rfList中
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }

        //执行所有超时的响应future的回调方法
        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    /**
     * 同步发送消息
     *
     * @param channel 通道
     * @param request 请求命令
     * @param timeoutMillis 同步调用的超时参数
     * @return 响应命令
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        //请求id
        final int opaque = request.getOpaque();

        try {
            //构造响应future
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            //将响应future放入responseTable中
            this.responseTable.put(opaque, responseFuture);
            //通道地址
            final SocketAddress addr = channel.remoteAddress();
            //发送命令，并注册发送完成监听器
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    //操作完成，并且发送成功
                    if (f.isSuccess()) {
                        //如果发送成功，设置sendRequestOK为true
                        responseFuture.setSendRequestOK(true);
                        //直接返回，等待响应
                        return;
                    } else {
                        //如果发送失败，设置sendRequestOK为false
                        responseFuture.setSendRequestOK(false);
                    }

                    //将响应future从responseTable中移除
                    responseTable.remove(opaque);
                    //设置失败原因
                    responseFuture.setCause(f.cause());
                    //唤醒等待线程
                    responseFuture.putResponse(null);
                    //打印日志
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });

            //等待远程响应
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    //发送成功响应超时
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis,
                        responseFuture.getCause());
                } else {
                    //发送异常
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }
            //返回响应命令
            return responseCommand;
        } finally {
            //最后将响应future从responseTable中移除
            this.responseTable.remove(opaque);
        }
    }

    /**
     * 异步发送
     *
     * @param invokeCallback 回调方法
     * @param channel 远程通道
     * @param request 请求命令
     * @param timeoutMillis 超时时间
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        //命令开始执行时间
        long beginStartTime = System.currentTimeMillis();
        //获取到请求id
        final int opaque = request.getOpaque();
        //获取信号量
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            //只能释放一次信号量
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            //耗费的时间
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeoutMillis < costTime) {
                //释放信号量
                once.release();
                //超时抛远程超时异常
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }

            //构造响应future，包含回调方法
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback, once);
            //响应future放入responseTable中
            this.responseTable.put(opaque, responseFuture);
            try {
                //发送请求并且注册命令发送完成的监听器
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            //设置发送成功
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        //请求失败
                        requestFail(opaque);
                        //请求失败打印日志
                        log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                    }
                });
            } catch (Exception e) {
                //出现异常释放信号量
                responseFuture.release();
                //打印日志
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                //抛出发送请求异常
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                //超时并且未获取到信号量，表明请求太多
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                    String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                        timeoutMillis,
                        this.semaphoreAsync.getQueueLength(),
                        this.semaphoreAsync.availablePermits()
                    );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    private void requestFail(final int opaque) {
        //将请求从responseTable中移除
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            //设置请求失败
            responseFuture.setSendRequestOK(false);
            //唤醒等待线程
            responseFuture.putResponse(null);
            try {
                //执行ResponseFuture的回调方法
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                //出现异常打印日志
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                //释放信号量
                responseFuture.release();
            }
        }
    }

    /**
     * 指定的通道的请求标记为失败,立即调用失败回调
     *
     * @param channel 已经关闭的通道
     */
    protected void failFast(final Channel channel) {
        Iterator<Entry<Integer, ResponseFuture>> it = responseTable.entrySet().iterator();
        //遍历所有的未响应请求
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> entry = it.next();
            //获取该通道未响应的请求
            if (entry.getValue().getProcessChannel() == channel) {
                //请求id
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    /**
     * 单向发送消息
     *
     * @param channel 通道
     * @param request 请求命令
     * @param timeoutMillis 超时时间
     */
    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        //标识请求是单向请求
        request.markOnewayRPC();
        //尝试获取信号量
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        //获取信号量成功
        if (acquired) {
            //信号量只释放一次，幂等操作
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                //发送命令，并注册操作完成监听器
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        //释放信号量
                        once.release();
                        //如果发送失败，发送命令失败，记录日志
                        if (!f.isSuccess()) {
                            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                //释放信号量
                once.release();
                //打印日志
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                //发送命令失败
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            //如果超时时间小于0，表明系统比较繁忙，没有获取到信号量，直接报RemotingTooMuchRequestException
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {//如果超时时间大于0，表明一定的时间内没有获取到信号量，报超时异常
                String info = String.format(
                    "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                    timeoutMillis,
                    this.semaphoreOneway.getQueueLength(),
                    this.semaphoreOneway.availablePermits()
                );
                //打印日志
                log.warn(info);
                //抛出超时异常
                throw new RemotingTimeoutException(info);
            }
        }
    }

    /**
     * netty事件执行器
     */
    class NettyEventExecutor extends ServiceThread {
        //存放事件队列
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        //存放事件队列的最大大小
        private final int maxSize = 10000;

        /**
         * 存放netty事件
         */
        public void putNettyEvent(final NettyEvent event) {
            //存放事件队列中的元素小于最大的大小
            if (this.eventQueue.size() <= maxSize) {
                //将netty事件加到队列中
                this.eventQueue.add(event);
            } else {
                //否则打印日志
                log.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            //获取通道事件监听器
            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    //从事件队列获取事件
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    //如果事件和通道事件监听器不为空
                    if (event != null && listener != null) {
                        //事件类型
                        switch (event.getType()) {
                            //通道空闲事件
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            //通道关闭事件
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            //通道连接事件
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            //通道异常事件
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
