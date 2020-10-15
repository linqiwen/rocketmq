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
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.Properties;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_AUTHSERVER;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_CERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_KEYPASSWORD;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_KEYPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_TRUSTCERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_AUTHCLIENT;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_CERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_KEYPASSWORD;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_KEYPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_NEED_CLIENT_AUTH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_TRUSTCERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_TEST_MODE_ENABLE;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientAuthServer;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientKeyPassword;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientKeyPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientTrustCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerAuthClient;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerKeyPassword;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerKeyPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerNeedClientAuth;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerTrustCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsTestModeEnable;

public class TlsHelper {

    public interface DecryptionStrategy {
        /**
         * Decrypt the target encrpted private key file.
         *
         * @param privateKeyEncryptPath A pathname string
         * @param forClient tells whether it's a client-side key file
         * @return An input stream for a decrypted key file
         * @throws IOException if an I/O error has occurred
         */
        InputStream decryptPrivateKey(String privateKeyEncryptPath, boolean forClient) throws IOException;
    }

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static DecryptionStrategy decryptionStrategy = new DecryptionStrategy() {
        @Override
        public InputStream decryptPrivateKey(final String privateKeyEncryptPath,
            final boolean forClient) throws IOException {
            return new FileInputStream(privateKeyEncryptPath);
        }
    };


    public static void registerDecryptionStrategy(final DecryptionStrategy decryptionStrategy) {
        TlsHelper.decryptionStrategy = decryptionStrategy;
    }

    public static SslContext buildSslContext(boolean forClient) throws IOException, CertificateException {
        File configFile = new File(TlsSystemConfig.tlsConfigFile);
        //从配置文件中提取TLS配置
        extractTlsConfigFromFile(configFile);
        //日志打印最后使用Tls配置
        logTheFinalUsedTlsConfig();

        SslProvider provider;
        if (OpenSsl.isAvailable()) {
            //如果OPENSSL可用
            provider = SslProvider.OPENSSL;
            LOGGER.info("Using OpenSSL provider");
        } else {
            //如果OPENSSL不可用，使用JDK的SSL
            provider = SslProvider.JDK;
            LOGGER.info("Using JDK SSL provider");
        }

        //判断是否是客户端
        if (forClient) {
            //确定初始化TLS上下文时是否使用测试模式
            if (tlsTestModeEnable) {
                //创建客户端SSL上下文
                return SslContextBuilder
                    .forClient()
                    .sslProvider(SslProvider.JDK)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
            } else {
                SslContextBuilder sslContextBuilder = SslContextBuilder.forClient().sslProvider(SslProvider.JDK);


                //确定是否严格验证服务器终结点的证书
                if (!tlsClientAuthServer) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    if (!isNullOrEmpty(tlsClientTrustCertPath)) {
                        sslContextBuilder.trustManager(new File(tlsClientTrustCertPath));
                    }
                }
                //创建客户端SSL上下文
                return sslContextBuilder.keyManager(
                    !isNullOrEmpty(tlsClientCertPath) ? new FileInputStream(tlsClientCertPath) : null,
                    !isNullOrEmpty(tlsClientKeyPath) ? decryptionStrategy.decryptPrivateKey(tlsClientKeyPath, true) : null,
                    !isNullOrEmpty(tlsClientKeyPassword) ? tlsClientKeyPassword : null)
                    .build();
            }
        } else {
            //确定初始化TLS上下文时是否使用测试模式
            if (tlsTestModeEnable) {
                SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
                return SslContextBuilder
                    .forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                    .sslProvider(SslProvider.JDK)
                    //客户端的验证方式
                    .clientAuth(ClientAuth.OPTIONAL)
                    .build();
            } else {
                SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(
                    !isNullOrEmpty(tlsServerCertPath) ? new FileInputStream(tlsServerCertPath) : null,
                    !isNullOrEmpty(tlsServerKeyPath) ? decryptionStrategy.decryptPrivateKey(tlsServerKeyPath, false) : null,
                    !isNullOrEmpty(tlsServerKeyPassword) ? tlsServerKeyPassword : null)
                    .sslProvider(provider);

                if (!tlsServerAuthClient) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    if (!isNullOrEmpty(tlsServerTrustCertPath)) {
                        sslContextBuilder.trustManager(new File(tlsServerTrustCertPath));
                    }
                }

                sslContextBuilder.clientAuth(parseClientAuthMode(tlsServerNeedClientAuth));
                return sslContextBuilder.build();
            }
        }
    }

    /**
     * 从配置文件中提取TLS配置
     */
    private static void extractTlsConfigFromFile(final File configFile) {
        if (!(configFile.exists() && configFile.isFile() && configFile.canRead())) {
            //文件不存在，或者不是文件，或者不可读直接返回
            LOGGER.info("Tls config file doesn't exist, skip it");
            return;
        }

        //TLS配置文件中的属性
        Properties properties;
        properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(configFile);
            //读取配置文件中的属性到properties中
            properties.load(inputStream);
        } catch (IOException ignore) {
        } finally {
            if (null != inputStream) {
                try {
                    //关闭文件流
                    inputStream.close();
                } catch (IOException ignore) {
                }
            }
        }

        //设置确定初始化TLS上下文时是否使用测试模式配置
        tlsTestModeEnable = Boolean.parseBoolean(properties.getProperty(TLS_TEST_MODE_ENABLE, String.valueOf(tlsTestModeEnable)));
        //设置tls服务器需要客户端身份验证方式配置
        tlsServerNeedClientAuth = properties.getProperty(TLS_SERVER_NEED_CLIENT_AUTH, tlsServerNeedClientAuth);
        //设置服务器端私钥的存储路径配置
        tlsServerKeyPath = properties.getProperty(TLS_SERVER_KEYPATH, tlsServerKeyPath);
        //设置服务器端私有密匙的密码配置
        tlsServerKeyPassword = properties.getProperty(TLS_SERVER_KEYPASSWORD, tlsServerKeyPassword);
        //设置PEM格式的服务器端X.509证书链的存储路径配置
        tlsServerCertPath = properties.getProperty(TLS_SERVER_CERTPATH, tlsServerCertPath);
        //设置确定是否严格验证客户端端点的证书配置
        tlsServerAuthClient = Boolean.parseBoolean(properties.getProperty(TLS_SERVER_AUTHCLIENT, String.valueOf(tlsServerAuthClient)));
        //用于验证客户端终结点证书的受信任证书的存储路径
        tlsServerTrustCertPath = properties.getProperty(TLS_SERVER_TRUSTCERTPATH, tlsServerTrustCertPath);
        //客户端私钥的存储路径
        tlsClientKeyPath = properties.getProperty(TLS_CLIENT_KEYPATH, tlsClientKeyPath);
        //客户端私钥的密码
        tlsClientKeyPassword = properties.getProperty(TLS_CLIENT_KEYPASSWORD, tlsClientKeyPassword);
        //PEM格式客户端X.509证书链的存储路径
        tlsClientCertPath = properties.getProperty(TLS_CLIENT_CERTPATH, tlsClientCertPath);
        //确定是否严格验证服务器终结点的证书
        tlsClientAuthServer = Boolean.parseBoolean(properties.getProperty(TLS_CLIENT_AUTHSERVER, String.valueOf(tlsClientAuthServer)));
        //正在验证终结点服务器的证书的受信任路径
        tlsClientTrustCertPath = properties.getProperty(TLS_CLIENT_TRUSTCERTPATH, tlsClientTrustCertPath);
    }

    /**
     * 日志打印最后使用Tls配置
     */
    private static void logTheFinalUsedTlsConfig() {
        LOGGER.info("Log the final used tls related configuration");
        LOGGER.info("{} = {}", TLS_TEST_MODE_ENABLE, tlsTestModeEnable);
        LOGGER.info("{} = {}", TLS_SERVER_NEED_CLIENT_AUTH, tlsServerNeedClientAuth);
        LOGGER.info("{} = {}", TLS_SERVER_KEYPATH, tlsServerKeyPath);
        LOGGER.info("{} = {}", TLS_SERVER_KEYPASSWORD, tlsServerKeyPassword);
        LOGGER.info("{} = {}", TLS_SERVER_CERTPATH, tlsServerCertPath);
        LOGGER.info("{} = {}", TLS_SERVER_AUTHCLIENT, tlsServerAuthClient);
        LOGGER.info("{} = {}", TLS_SERVER_TRUSTCERTPATH, tlsServerTrustCertPath);

        LOGGER.info("{} = {}", TLS_CLIENT_KEYPATH, tlsClientKeyPath);
        LOGGER.info("{} = {}", TLS_CLIENT_KEYPASSWORD, tlsClientKeyPassword);
        LOGGER.info("{} = {}", TLS_CLIENT_CERTPATH, tlsClientCertPath);
        LOGGER.info("{} = {}", TLS_CLIENT_AUTHSERVER, tlsClientAuthServer);
        LOGGER.info("{} = {}", TLS_CLIENT_TRUSTCERTPATH, tlsClientTrustCertPath);
    }

    private static ClientAuth parseClientAuthMode(String authMode) {
        if (null == authMode || authMode.trim().isEmpty()) {
            return ClientAuth.NONE;
        }

        for (ClientAuth clientAuth : ClientAuth.values()) {
            if (clientAuth.name().equals(authMode.toUpperCase())) {
                return clientAuth;
            }
        }

        return ClientAuth.NONE;
    }

    /**
     * Determine if a string is {@code null} or {@link String#isEmpty()} returns {@code true}.
     */
    private static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }
}
