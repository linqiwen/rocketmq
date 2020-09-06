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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.JSON;
import java.nio.charset.Charset;

/**
 * 远程序列化
 */
public abstract class RemotingSerializable {
    /**
     * 编码和解码的字符集编码
     */
    private final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    /**
     * 将对象编码成字节数组
     *
     * @param obj 要编码的对象
     * @return 字节数组
     */
    public static byte[] encode(final Object obj) {
        //将对象转成字节json串
        final String json = toJson(obj, false);
        if (json != null) {
            //将json串转成字节数组
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    /**
     * 将对象转成json串
     *
     * @param obj 需要转成json串对象
     * @param prettyFormat 是否需要格式化
     * @return 对象json串
     */
    public static String toJson(final Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    /**
     * 将字节数组转成对象
     *
     * @param data 字节数组
     * @param classOfT 对象类型
     * @return classOfT实例
     */
    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        //将字节数组转成json串
        final String json = new String(data, CHARSET_UTF8);
        //将json串转成对象
        return fromJson(json, classOfT);
    }

    /**
     * 将json串转成T对象
     *
     * @param json json串
     * @param classOfT T类型
     * @return T对象
     */
    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

    /**
     * 编码
     *
     * @return 自身对象的字节数组
     */
    public byte[] encode() {
        final String json = this.toJson();
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public String toJson() {
        return toJson(false);
    }

    public String toJson(final boolean prettyFormat) {
        return toJson(this, prettyFormat);
    }
}
