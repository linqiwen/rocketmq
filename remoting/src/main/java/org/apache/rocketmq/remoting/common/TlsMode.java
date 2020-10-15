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

/**
 * 对于服务器，支持三种SSL模式：disabled、permissive和enforcing
 * <ol>
 *     <li><strong>禁用:</strong> 不支持SSL; 任何传入的SSL握手都将被拒绝，从而导致连接关闭</li>
 *     <li><strong>允许:</strong> SSL是可选的，也就是说，这种模式下的服务器可以使用或不使用SSL来服务客户机连接</li>
 *     <li><strong>强制:</strong> 需要SSL，即非SSL连接将被拒绝.</li>
 * </ol>
 */
public enum TlsMode {

    /**
     * 禁用
     */
    DISABLED("disabled"),
    /**
     * 允许
     */
    PERMISSIVE("permissive"),
    /**
     * 强制
     */
    ENFORCING("enforcing");

    private String name;

    TlsMode(String name) {
        this.name = name;
    }

    public static TlsMode parse(String mode) {
        for (TlsMode tlsMode : TlsMode.values()) {
            if (tlsMode.name.equals(mode)) {
                return tlsMode;
            }
        }

        return PERMISSIVE;
    }

    public String getName() {
        return name;
    }
}
