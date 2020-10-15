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

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.remoting.common.TlsMode;

/**
 * TLS协议系统配置
 */
public class TlsSystemConfig {
    /**
     * TLS服务模式
     */
    public static final String TLS_SERVER_MODE = "tls.server.mode";
    public static final String TLS_ENABLE = "tls.enable";
    public static final String TLS_CONFIG_FILE = "tls.config.file";
    public static final String TLS_TEST_MODE_ENABLE = "tls.test.mode.enable";

    public static final String TLS_SERVER_NEED_CLIENT_AUTH = "tls.server.need.client.auth";
    public static final String TLS_SERVER_KEYPATH = "tls.server.keyPath";
    public static final String TLS_SERVER_KEYPASSWORD = "tls.server.keyPassword";
    public static final String TLS_SERVER_CERTPATH = "tls.server.certPath";
    public static final String TLS_SERVER_AUTHCLIENT = "tls.server.authClient";
    public static final String TLS_SERVER_TRUSTCERTPATH = "tls.server.trustCertPath";

    public static final String TLS_CLIENT_KEYPATH = "tls.client.keyPath";
    public static final String TLS_CLIENT_KEYPASSWORD = "tls.client.keyPassword";
    public static final String TLS_CLIENT_CERTPATH = "tls.client.certPath";
    public static final String TLS_CLIENT_AUTHSERVER = "tls.client.authServer";
    public static final String TLS_CLIENT_TRUSTCERTPATH = "tls.client.trustCertPath";


    /**
     * 决定客户端侧是否使用SSL, 包括SDK客户端和BrokerRouterAPI
     */
    public static boolean tlsEnable = Boolean.parseBoolean(System.getProperty(TLS_ENABLE, "false"));


    /**
     * 确定初始化TLS上下文时是否使用测试模式
     */
    public static boolean tlsTestModeEnable = Boolean.parseBoolean(System.getProperty(TLS_TEST_MODE_ENABLE, "true"));

    /**
     * 指示{@link javax.net.ssl.SSLEngine}关于客户端身份验证。
     * 这个配置项实际上只在构建服务器端{@linksslcontext}时应用，并且可以设置为none、require或optional
     *
     * @see ClientAuth
     */
    public static String tlsServerNeedClientAuth = System.getProperty(TLS_SERVER_NEED_CLIENT_AUTH, "none");
    /**
     * 服务器端私钥的存储路径
     */
    public static String tlsServerKeyPath = System.getProperty(TLS_SERVER_KEYPATH, null);

    /**
     * 服务器端私有密匙的密码
     */
    public static String tlsServerKeyPassword = System.getProperty(TLS_SERVER_KEYPASSWORD, null);

    /**
     * PEM格式的服务器端X.509证书链的存储路径
     */
    public static String tlsServerCertPath = System.getProperty(TLS_SERVER_CERTPATH, null);

    /**
     * 确定是否严格验证客户端端点的证书
     */
    public static boolean tlsServerAuthClient = Boolean.parseBoolean(System.getProperty(TLS_SERVER_AUTHCLIENT, "false"));

    /**
     * 用于验证客户端终结点证书的受信任证书的存储路径
     */
    public static String tlsServerTrustCertPath = System.getProperty(TLS_SERVER_TRUSTCERTPATH, null);

    /**
     * 客户端私钥的存储路径
     */
    public static String tlsClientKeyPath = System.getProperty(TLS_CLIENT_KEYPATH, null);

    /**
     * 客户端私钥的密码
     */
    public static String tlsClientKeyPassword = System.getProperty(TLS_CLIENT_KEYPASSWORD, null);

    /**
     * PEM格式客户端X.509证书链的存储路径
     */
    public static String tlsClientCertPath = System.getProperty(TLS_CLIENT_CERTPATH, null);

    /**
     * 确定是否严格验证服务器终结点的证书
     */
    public static boolean tlsClientAuthServer = Boolean.parseBoolean(System.getProperty(TLS_CLIENT_AUTHSERVER, "false"));

    /**
     * 正在验证终结点服务器的证书的受信任路径
     */
    public static String tlsClientTrustCertPath = System.getProperty(TLS_CLIENT_TRUSTCERTPATH, null);

    /**
     * 对于服务器，支持三种SSL模式：disabled、permissive和enforcing
     * 对应客户端，使用{@link TlsSystemConfig#tlsEnable}决定是否使用SSL
     * <ol>
     *     <li><strong>禁用:</strong> 不支持SSL; 任何传入的SSL握手都将被拒绝，从而导致连接关闭</li>
     *     <li><strong>允许:</strong> SSL是可选的，也就是说，这种模式下的服务器可以使用或不使用SSL来服务客户机连接</li>
     *     <li><strong>强制:</strong> 需要SSL，即非SSL连接将被拒绝.</li>
     * </ol>
     */
    public static TlsMode tlsMode = TlsMode.parse(System.getProperty(TLS_SERVER_MODE, "permissive"));

    /**
     * 一个配置文件，用于存储上述与TLS相关的配置,
     * 除了 {@link TlsSystemConfig#tlsMode} and {@link TlsSystemConfig#tlsEnable}
     */
    public static String tlsConfigFile = System.getProperty(TLS_CONFIG_FILE, "/etc/rocketmq/tls.properties");
}
