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
package org.apache.rocketmq.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 远程工具类
 */
public class RemotingUtil {

    /**
     * 操作系统名称
     */
    public static final String OS_NAME = System.getProperty("os.name");

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * linux系统
     */
    private static boolean isLinuxPlatform = false;
    /**
     * windows系统
     */
    private static boolean isWindowsPlatform = false;

    static {
        if (OS_NAME != null && OS_NAME.toLowerCase().contains("linux")) {
            isLinuxPlatform = true;
        }

        if (OS_NAME != null && OS_NAME.toLowerCase().contains("windows")) {
            isWindowsPlatform = true;
        }
    }

    /**
     * 判断是否windows系统
     *
     * @return {@code true} windows系统
     */
    public static boolean isWindowsPlatform() {
        return isWindowsPlatform;
    }

    /**
     * 开启选择器
     *
     * @return 选择器
     */
    public static Selector openSelector() throws IOException {
        Selector result = null;

        //判断是否是linux系统
        if (isLinuxPlatform()) {
            try {
                //使用反射获取性能更高的EPollSelectorProvider
                final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                if (providerClazz != null) {
                    try {
                        final Method method = providerClazz.getMethod("provider");
                        if (method != null) {
                            final SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
                            if (selectorProvider != null) {
                                result = selectorProvider.openSelector();
                            }
                        }
                    } catch (final Exception e) {
                        log.warn("Open ePoll Selector for linux platform exception", e);
                    }
                }
            } catch (final Exception e) {
                // ignore
            }
        }

        //如果不是linux系统调用Selector开启选择器
        if (result == null) {
            result = Selector.open();
        }

        return result;
    }

    /**
     * 判断是否linux系统
     *
     * @return {@code true} linux系统
     */
    public static boolean isLinuxPlatform() {
        return isLinuxPlatform;
    }


    public static String getLocalAddress() {
        try {
            // Traversal Network interface to get the first non-loopback and non-private address
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            //ipv4地址列表
            ArrayList<String> ipv4Result = new ArrayList<String>();
            //ipv6地址列表
            ArrayList<String> ipv6Result = new ArrayList<String>();
            //遍历所有的地址
            while (enumeration.hasMoreElements()) {
                final NetworkInterface networkInterface = enumeration.nextElement();
                final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
                while (en.hasMoreElements()) {
                    //获取地址
                    final InetAddress address = en.nextElement();
                    //判断是否是个环回地址
                    if (!address.isLoopbackAddress()) {
                        //如果不是环回地址，是否是ipv6
                        if (address instanceof Inet6Address) {
                            //如果是ipv6将地址加到ipv6地址列表中
                            ipv6Result.add(normalizeHostAddress(address));
                        } else {
                            //如果是ipv4地址加到ipv4地址列表中
                            ipv4Result.add(normalizeHostAddress(address));
                        }
                    }
                }
            }

            // ipv4地址列表不为空
            if (!ipv4Result.isEmpty()) {
                for (String ip : ipv4Result) {
                    if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
                        continue;
                    }

                    return ip;
                }

                return ipv4Result.get(ipv4Result.size() - 1);
            } else if (!ipv6Result.isEmpty()) {
                return ipv6Result.get(0);
            }
            //If failed to find,fall back to localhost
            final InetAddress localHost = InetAddress.getLocalHost();
            return normalizeHostAddress(localHost);
        } catch (Exception e) {
            log.error("Failed to obtain local address", e);
        }

        return null;
    }

    /**
     * InetAddress转成串地址
     *
     * @param localHost localHost
     * @return 地址串
     */
    public static String normalizeHostAddress(final InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        } else {
            return localHost.getHostAddress();
        }
    }

    /**
     * 将串转成SocketAddress
     */
    public static SocketAddress string2SocketAddress(final String addr) {
        String[] s = addr.split(":");
        InetSocketAddress isa = new InetSocketAddress(s[0], Integer.parseInt(s[1]));
        return isa;
    }

    public static String socketAddress2String(final SocketAddress addr) {
        StringBuilder sb = new StringBuilder();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
        sb.append(inetSocketAddress.getAddress().getHostAddress());
        sb.append(":");
        sb.append(inetSocketAddress.getPort());
        return sb.toString();
    }

    public static SocketChannel connect(SocketAddress remote) {
        return connect(remote, 1000 * 5);
    }

    /**
     * 连接远程
     *
     * @param remote 远程地址
     * @param timeoutMillis 连接远程服务的超时时间
     * @return 远程通道
     */
    public static SocketChannel connect(SocketAddress remote, final int timeoutMillis) {
        SocketChannel sc = null;
        try {
            //开启socket
            sc = SocketChannel.open();
            //调整这个通道的阻塞模式
            sc.configureBlocking(true);
            //1、设置 l_onoff为0，则该选项关闭，l_linger的值被忽略，等于内核缺省情况，close调用会立即返回给调用者，如果可能将会传输任何未发送的数据；
            //2、设置 l_onoff为非0，l_linger为0，则套接口关闭时TCP夭折连接，TCP将丢弃保留在套接口发送缓冲区中的任何数据并发送一个RST给对方，而不是通常的四分组终止序列，这避免了TIME_WAIT状态；
            //3、设置 l_onoff为非0，l_linger为非0，当套接口关闭时内核将拖延一段时间（由l_linger决定）。如果套接口缓冲区中仍残留数据，进程将处于睡眠状态，直到（a）所有数据发送完且被对方确认，
            // 之后进行正常的终止序列（描述字访问计数为0）或（b）延迟时间到。此种情况下，应用程序检查close的返回值是非常重要的，如果在数据发送完并被确认前时间到，
            // close将返回EWOULDBLOCK错误且套接口发送缓冲区中的任何数据都丢失。close的成功返回仅告诉我们发送的数据（和FIN）已由对方TCP确认，
            // 它并不能告诉我们对方应用进程是否已读了数据。如果套接口设为非阻塞的，它将不等待close完成
            sc.socket().setSoLinger(false, -1);
            //启动TCP_NODELAY，就意味着禁用了Nagle算法，允许小包的发送。对于延时敏感型，同时数据传输量比较小的应用，开启TCP_NODELAY选项无疑是一个正确的选择
            sc.socket().setTcpNoDelay(true);
            //接收的缓冲区大小
            sc.socket().setReceiveBufferSize(1024 * 64);
            //发送的缓冲区大小
            sc.socket().setSendBufferSize(1024 * 64);
            //连接远程
            sc.socket().connect(remote, timeoutMillis);
            //调整这个通道的阻塞模式
            sc.configureBlocking(false);
            return sc;
        } catch (Exception e) {
            if (sc != null) {
                try {
                    sc.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        return null;
    }

    /**
     * 关闭通道
     *
     * @param channel 通道
     */
    public static void closeChannel(Channel channel) {
        //获取通道的远程地址
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        //关闭通道，并注册通道关闭监听器
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                    future.isSuccess());
            }
        });
    }

}
