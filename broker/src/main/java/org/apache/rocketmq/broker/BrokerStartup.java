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
package org.apache.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

/**
 * broker启动类
 */
public class BrokerStartup {
    /**
     * 属性列表
     */
    public static Properties properties = null;
    /**
     * 命令行
     */
    public static CommandLine commandLine = null;
    /**
     * 配置文件
     */
    public static String configFile = null;
    /**
     * 日志
     */
    public static InternalLogger log;

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {
            //启动broker启动器
            controller.start();

            //启动broker提示
            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            //broker控制器的nameSrv地址不为空，提示加上nameSrv地址
            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            //打印日志
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            //出现异常直接退出jvm
            System.exit(-1);
        }

        return null;
    }

    /**
     * 关闭BrokerController
     */
    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    /**
     * 创建BrokerController
     */
    public static BrokerController createBrokerController(String[] args) {
        //设置版本号
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        //获取Socket发送的缓冲区大小
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            //如果没有设置设置成128M
            NettySystemConfig.socketSndbufSize = 131072;
        }

        //获取Socket接收的缓冲区大小
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            //如果没有设置设置成128M
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            //PackageConflictDetect.detectFastjson();
            //生成命令行选项
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            //解析命令行
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                //命令行为空直接退出，不同的状态码可以判断是因为什么原因而退出
                System.exit(-1);
            }

            //创建Broker配置
            final BrokerConfig brokerConfig = new BrokerConfig();
            //创建netty服务端配置
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            //创建netty客户端配置
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();
            //设置TLS开关
            nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
                String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));
            //设置服务端的监听端口
            nettyServerConfig.setListenPort(10911);
            //创建消息存储配置
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                //如果是从机，内存中访问消息的最大比率减10
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                //重新设置内存中访问消息的最大比率
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            //判断命令行是否有配置文件选项
            if (commandLine.hasOption('c')) {
                //获取文件路径
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    //获取配置文件流
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    //创建属性列表
                    properties = new Properties();
                    //将文件属性读到属性列表中
                    properties.load(in);
                    //设置系统环境属性
                    properties2SystemEnv(properties);
                    //设置Broker配置
                    MixAll.properties2Object(properties, brokerConfig);
                    //设置netty服务端配置
                    MixAll.properties2Object(properties, nettyServerConfig);
                    //设置组netty客户端配置
                    MixAll.properties2Object(properties, nettyClientConfig);
                    //设置消息存储配置
                    MixAll.properties2Object(properties, messageStoreConfig);
                    //设置broker配置文件路径
                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    //关闭配置文件流
                    in.close();
                }
            }

            //设置Broker配置
            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            //如果没有设置rocketMq主目录，直接退出
            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                //直接退出，不同的状态码可以判断是因为什么原因而退出
                System.exit(-2);
            }

            //-------------------------------------验证nameSrv地址-------------------------------------------
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    //获取所有的nameSrv地址
                    String[] addrArray = namesrvAddr.split(";");
                    //遍历nameSrv地址
                    for (String addr : addrArray) {
                        //创建SocketAddress，如果地址不合法会抛异常
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    //直接退出，不同的状态码可以判断是因为什么原因而退出
                    System.exit(-3);
                }
            }

            switch (messageStoreConfig.getBrokerRole()) {
                //异步主
                case ASYNC_MASTER:
                //同步主
                case SYNC_MASTER:
                    //brokerId都设置成0
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                //从
                case SLAVE:
                    //从brokerId
                    if (brokerConfig.getBrokerId() <= 0) {
                        //出现异常打印日志
                        System.out.printf("Slave's brokerId must be > 0");
                        //直接退出，不同的状态码可以判断是因为什么原因而退出
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }

            //设置ha的监听端口
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            //设置loggerContext到JoranConfigurator
            configurator.setContext(lc);
            //清除loggerContext已加载配置，重新加载
            lc.reset();
            //加载默认配置
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");

            //命令行含有p选项，打印日志并退出
            if (commandLine.hasOption('p')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                //打印Broker配置所有属性
                MixAll.printObjectProperties(console, brokerConfig);
                //打印netty服务端配置所有属性
                MixAll.printObjectProperties(console, nettyServerConfig);
                //打印netty客户端配置所有属性
                MixAll.printObjectProperties(console, nettyClientConfig);
                //打印消息存储配置所有属性
                MixAll.printObjectProperties(console, messageStoreConfig);
                //正常退出
                System.exit(0);
            //命令行含有m选项，打印日志并退出
            } else if (commandLine.hasOption('m')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                //打印Broker配置重要属性
                MixAll.printObjectProperties(console, brokerConfig, true);
                //打印netty服务端配置重要属性
                MixAll.printObjectProperties(console, nettyServerConfig, true);
                //打印netty客户端配置重要属性
                MixAll.printObjectProperties(console, nettyClientConfig, true);
                //打印消息存储配置重要属性
                MixAll.printObjectProperties(console, messageStoreConfig, true);
                //正常退出
                System.exit(0);
            }

            log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            //打印Broker配置所有属性
            MixAll.printObjectProperties(log, brokerConfig);
            //打印netty服务端配置所有属性
            MixAll.printObjectProperties(log, nettyServerConfig);
            //打印netty客户端配置所有属性
            MixAll.printObjectProperties(log, nettyClientConfig);
            //打印消息存储配置所有属性
            MixAll.printObjectProperties(log, messageStoreConfig);

            final BrokerController controller = new BrokerController(
                brokerConfig,
                nettyServerConfig,
                nettyClientConfig,
                messageStoreConfig);
            // remember all configs to prevent discard
            controller.getConfiguration().registerConfig(properties);

            //初始化BrokerController
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            //注册钩子方法，jvm关闭时执行
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                //是否关闭标识
                private volatile boolean hasShutdown = false;
                //关闭次数
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        //打印日志
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        //钩子方法已经执行过一次直接跳过
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            //执行broker控制器的开始时间
                            long beginTime = System.currentTimeMillis();
                            //执行broker控制器的关闭方法
                            controller.shutdown();
                            //执行broker控制器的耗时时间
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            //打印日志
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));
            //返回broker控制器
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 设置系统环境属性
     */
    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        //rmq地址服务器域
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        //rmq地址服务器子组
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        //命令行的配置文件选项
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        //非必填
        opt.setRequired(false);
        //将选项加入到选项列表中
        options.addOption(opt);

        //打印所有的配置项
        opt = new Option("p", "printConfigItem", false, "Print all config item");
        //非必填
        opt.setRequired(false);
        //将选项加入到选项列表中
        options.addOption(opt);

        //打印重要的配置项
        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        //非必填
        opt.setRequired(false);
        //将选项加入到选项列表中
        options.addOption(opt);
        //返回选项列表
        return options;
    }
}
