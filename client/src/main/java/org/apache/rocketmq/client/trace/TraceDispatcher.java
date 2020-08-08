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
package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import java.io.IOException;

/**
 * 异步传输数据的接口
 */
public interface TraceDispatcher {

    /**
     * 异步传输数据模块进行初始化
     *
     * @param nameSrvAddr nameSrv地址
     * @param accessChannel 访问通道
     */
    void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException;

    /**
     * 附加转换数据
     * @param ctx 数据信息
     * @return {@code true}附加转换数据成功
     */
    boolean append(Object ctx);

    /**
     * 写入刷新操作
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * 关闭跟踪钩
     */
    void shutdown();
}
