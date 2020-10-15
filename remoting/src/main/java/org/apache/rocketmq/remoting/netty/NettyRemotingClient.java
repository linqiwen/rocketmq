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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * netty远程客户端
 */
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * 等待获取锁的超时时间
     */
    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    /**
     * netty客户端配置
     */
    private final NettyClientConfig nettyClientConfig;
    /**
     * 启动器
     */
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    /**
     * channelTables操作锁
     */
    private final Lock lockChannelTables = new ReentrantLock();
    /**
     * ChannelWrapper的Map,key:地址,value:channel包装类
     */
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();

    /**
     * 轮询待响应列表定时器
     */
    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    /**
     * 所有的nameSrv地址列表
     */
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<String>();
    /**
     * 为了随机的选择namesrv地址
     */
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    /**
     * namesrv通道锁
     */
    private final Lock lockNamesrvChannel = new ReentrantLock();

    /**
     * 公共的执行器
     */
    private final ExecutorService publicExecutor;

    /**
     * 当远程服务响应时在执行器中调用回调方法
     */
    private ExecutorService callbackExecutor;
    /**
     * 通道事件监听器
     */
    private final ChannelEventListener channelEventListener;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });

        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                log.info("SSL enabled for client");
            } catch (IOException e) {
                log.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    private static int initValueIndex() {
        Random r = new Random();

        return Math.abs(r.nextInt() % 999) % 999;
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            nettyClientConfig.getClientWorkerThreads(),
            new ThreadFactory() {

                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                }
            });

        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                            log.info("Prepend SSL handler");
                        } else {
                            log.warn("Connections are insecure as SSLContext is null!");
                        }
                    }
                    pipeline.addLast(
                        defaultEventExecutorGroup,
                        new NettyEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });

        //每隔1秒轮询待响应列表
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    //定期调用此方法来扫描已过期的请求并使其过期
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    //打印日志
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

        //通道事件监听器不为空
        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }
    }

    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            for (ChannelWrapper cw : this.channelTables.values()) {
                this.closeChannel(null, cw.getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    /**
     * 关闭通道
     *
     * @param addr 远程地址
     * @param channel 远程通道
     */
    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel)
            //通道为空直接返回回去
            return;

        //获取远程地址
        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //是否需要从channelTables中移除通道
                    boolean removeItemFromTable = true;
                    //根据地址获取通道的包装类
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    //打印日志
                    log.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote, prevCW != null);

                    if (null == prevCW) {
                        //通道的包装类为空，打印日志
                        log.info("closeChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        //通道不相等，表明通道已经关闭，并且创建新的通道
                        log.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                            addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        //将通道从channelTables中移除
                        this.channelTables.remove(addrRemote);
                        //打印日志
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }

                    //关闭通道
                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    //出现异常打印日志
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    //释放锁
                    this.lockChannelTables.unlock();
                }
            } else {
                //获取锁超时，打印日志
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            //被中断，打印日志
            log.error("closeChannel exception", e);
        }
    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    /**
     * 关闭通道
     */
    public void closeChannel(final Channel channel) {
        //通道为空直接返回
        if (null == channel)
            return;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    //通道是否需要从列表中移除
                    boolean removeItemFromTable = true;
                    //需要关闭的通道包装类，从channelTables中寻找
                    ChannelWrapper prevCW = null;
                    //要关闭通道的远程地址
                    String addrRemote = null;
                    //从channelTables中寻找要关闭通道
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        //通道的远程地址
                        String key = entry.getKey();
                        //通道的包装类
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            //要关闭通道在channelTables中
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    //要关闭通道在table中不存在，表明已经被关闭
                    if (null == prevCW) {
                        log.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        //从table中移除通道
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        //关闭通道
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                log.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                this.namesrvAddrList.set(addrs);
            }
        }
    }


    /**
     * 同步发送命令
     *
     * @param addr 远程地址
     * @param request 请求命令
     * @param timeoutMillis 发送命令的超时时间
     */
    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        //处理的开始时间
        long beginStartTime = System.currentTimeMillis();
        //根据地址获取通道
        final Channel channel = this.getAndCreateChannel(addr);
        //如果通道存在并且活跃，对其进行处理
        if (channel != null && channel.isActive()) {
            try {
                //请求执行之前，执行所有钩子的doBeforeRequest方法
                doBeforeRpcHooks(addr, request);
                //当前耗费的时间
                long costTime = System.currentTimeMillis() - beginStartTime;
                //如果耗费的时间大于超时时间
                if (timeoutMillis < costTime) {
                    //抛出RemotingTimeoutException异常
                    throw new RemotingTimeoutException("invokeSync call timeout");
                }
                //执行命令
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis - costTime);
                //请求执行之后，执行所有钩子的doAfterResponse方法，可以不用再调RemotingHelper.parseChannelRemoteAddr(channel)，直接传addr就可以
                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(channel), request, response);
                //返回响应请求
                return response;
            } catch (RemotingSendRequestException e) {
                //出现异常打印日志
                log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                //关闭通道
                this.closeChannel(addr, channel);
                //抛出异常
                throw e;
            } catch (RemotingTimeoutException e) {
                //如果超时客户端关闭socket开关
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    //开关开，关闭通道
                    this.closeChannel(addr, channel);
                    //打印日志
                    log.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                }
                //打印日志
                log.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                //抛出异常
                throw e;
            }
        } else {
            //关闭通道
            this.closeChannel(addr, channel);
            //抛出连接异常
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 获取通道，获取不到创建新通道
     */
    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        if (null == addr) {
            //如果传入进来的地址为空，获取或创建nameServer通道
            return getAndCreateNameserverChannel();
        }

        //先从内存中获取包装的通道信息
        ChannelWrapper cw = this.channelTables.get(addr);
        //通道存在并且活跃的
        if (cw != null && cw.isOK()) {
            //返回通道信息
            return cw.getChannel();
        }
        //内存中不存在创建新通道
        return this.createChannel(addr);
    }

    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        //上次选择的nameSrv地址
        String addr = this.namesrvAddrChoosed.get();
        //选择nameSrv地址不为空
        if (addr != null) {
            //从内存中获取
            ChannelWrapper cw = this.channelTables.get(addr);
            //通道存在并且活跃的
            if (cw != null && cw.isOK()) {
                //返回通道信息
                return cw.getChannel();
            }
        }

        //否则获取到所有nameSrv地址列表
        final List<String> addrList = this.namesrvAddrList.get();
        //尝试加锁
        if (this.lockNamesrvChannel.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                //再次获取上次选择的nameSrv地址
                addr = this.namesrvAddrChoosed.get();
                //选择nameSrv地址不为空
                if (addr != null) {
                    //从内存中获取
                    ChannelWrapper cw = this.channelTables.get(addr);
                    //通道存在并且活跃的
                    if (cw != null && cw.isOK()) {
                        //返回通道信息
                        return cw.getChannel();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    //nameSrv地址列表不为空，遍历nameSrv地址列表
                    for (int i = 0; i < addrList.size(); i++) {
                        //namesrvIndex进行递增，随机取
                        int index = this.namesrvIndex.incrementAndGet();
                        //取绝对值，防止溢出
                        index = Math.abs(index);
                        //取模
                        index = index % addrList.size();
                        //获取到namesrv地址
                        String newAddr = addrList.get(index);
                        this.namesrvAddrChoosed.set(newAddr);
                        log.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                        Channel channelNew = this.createChannel(newAddr);
                        //通道创建成功
                        if (channelNew != null) {
                            //返回
                            return channelNew;
                        }
                    }
                }
            } catch (Exception e) {
                //出现异常打印日志
                log.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                //释放锁
                this.lockNamesrvChannel.unlock();
            }
        } else {
            //获取锁超时打印日志
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    /**
     * 创建通信的通道
     *
     * @param addr 地址
     * @return 通道
     */
    private Channel createChannel(final String addr) throws InterruptedException {
        //先获取地址所对应的通道
        ChannelWrapper cw = this.channelTables.get(addr);
        //地址所对应的通道存在，并且活跃，将其通道关闭，并从channelTables中移除
        if (cw != null && cw.isOK()) {
            //关闭通道，从channelTables中移除
            cw.getChannel().close();
            channelTables.remove(addr);
        }

        //尝试加锁
        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                boolean createNewConnection;
                //再次获取地址所对应的通道
                cw = this.channelTables.get(addr);
                if (cw != null) {

                    if (cw.isOK()) {
                        //地址所对应的通道存在，并且活跃，将其通道关闭，并从channelTables中移除
                        cw.getChannel().close();
                        //从channelTables中移除
                        this.channelTables.remove(addr);
                        //需重新创建个通道
                        createNewConnection = true;
                    } else if (!cw.getChannelFuture().isDone()) {//通道还未创建完成
                        createNewConnection = false;
                    } else {
                        //从channelTables中移除
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }

                if (createNewConnection) {
                    //创建新通道
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    //打印日志
                    log.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    //将通道进行包装
                    cw = new ChannelWrapper(channelFuture);
                    //加入到channelTables中
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                //出现异常打印日志
                log.error("createChannel: create channel exception", e);
            } finally {
                //释放锁
                this.lockChannelTables.unlock();
            }
        } else {
            //超时时间获取不到锁打印日志
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            //如果通道不为空
            ChannelFuture channelFuture = cw.getChannelFuture();
            //等待通道连接成功，连接超时时间3s
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (cw.isOK()) {
                    //通道连接成功打印日志
                    log.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    //通道连接失败打印日志
                    log.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.toString(), channelFuture.cause());
                }
            } else {
                //如果在超时时间通道还未连接成功，打印日志
                log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(),
                    channelFuture.toString());
            }
        }

        return null;
    }

    /**
     * 异步发送消息
     *
     * @param addr 请求地址
     * @param request 请求命令
     * @param timeoutMillis 超时时间
     * @param invokeCallback 回调方法
     */
    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        //开始执行时间
        long beginStartTime = System.currentTimeMillis();
        //获取通道，不存在创建
        final Channel channel = this.getAndCreateChannel(addr);
        //通道不为空，并且活跃进行处理
        if (channel != null && channel.isActive()) {
            try {
                //命令执行之前，执行钩子方法
                doBeforeRpcHooks(addr, request);
                //目前的耗费的时间
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    //如果耗费的时间大于超时时间，抛异常
                    throw new RemotingTooMuchRequestException("invokeAsync call timeout");
                }
                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, invokeCallback);
            } catch (RemotingSendRequestException e) {
                //发送异常，打印日志
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                //关闭通道
                this.closeChannel(addr, channel);
                //抛异常
                throw e;
            }
        } else {
            //通道不活跃或者为空，进行关闭
            this.closeChannel(addr, channel);
            //抛远程连接异常
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                //在命令之前执行所有钩子的doBeforeRequest方法
                doBeforeRpcHooks(addr, request);
                //向通道发送命令
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                //出现异常
                log.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                //关闭通道
                this.closeChannel(addr, channel);
                //抛异常
                throw e;
            }
        } else {
            //通道不可用，关闭通道
            this.closeChannel(addr, channel);
            //抛出远程连接异常
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            //如果传入的执行器为空使用公共的执行器
            executorThis = this.publicExecutor;
        }

        //将处理器和执行器组合起来
        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        //加入到处理器列表中
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }


    @Override
    public ExecutorService getCallbackExecutor() {
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    /**
     * channel包装类
     */
    static class ChannelWrapper {
        /**
         * 渠道future
         */
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        /**
         * 渠道不为空，并且渠道是活跃
         */
        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        /**
         * 当且仅当I/O线程将立即执行写入操作的请求时，返回true。
         * 此方法返回false时发出的任何写入请求都将排队，直到I/O线程准备好处理排队的写入请求
         */
        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        /**
         * 获取渠道
         */
        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        /**
         * 获取渠道future
         */
        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }
    }

    /**
     * 客户端远程服务响应处理器
     * <p>
     *     设置远程命令并且唤醒等待线程
     * </p>
     */
    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        /**
         * 远程通道有可读信息
         *
         * @param ctx 通道处理器上下文
         * @param msg 远程消息
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            //处理远程命令
            processMessageReceived(ctx, msg);
        }
    }

    /**
     * netty连接管理处理程序
     */
    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            //等待下一个事件
            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                //如果netty的通道事件监听不为空，把连接事件交给执行器
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            //打印日志
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            //关闭通道
            closeChannel(ctx.channel());
            //等待下一个事件
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                //如果netty的通道事件监听不为空，把连接事件交给执行器
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        /**
         * 通道关闭
         *
         * @param ctx 通道处理器上下文
         * @param promise 可写的ChannelFuture
         */
        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            //远程地址
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            //打印日志
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            //关闭通道
            closeChannel(ctx.channel());
            //等待下一个事件
            super.close(ctx, promise);
            NettyRemotingClient.this.failFast(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                //事件监听处理器不为空，发送通道关闭事件
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        /**
         * 用户事件触发
         *
         * @param ctx 通道处理程序上下文
         * @param evt 事件
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                //通道空闲超过最大时间，关闭通道
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}
