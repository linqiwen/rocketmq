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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.PermName;

/**
 * 主题配置
 */
public class TopicConfig {
    /**
     * 分隔符
     */
    private static final String SEPARATOR = " ";
    /**
     * 默认的读队列数
     */
    public static int defaultReadQueueNums = 16;
    /**
     * 默认的写队列数
     */
    public static int defaultWriteQueueNums = 16;
    /**
     * 主题名称
     */
    private String topicName;
    /**
     * 读队列数
     */
    private int readQueueNums = defaultReadQueueNums;
    /**
     * 写队列数
     */
    private int writeQueueNums = defaultWriteQueueNums;

    /**
     * topic权限
     */
    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;
    /**
     * 主题过滤类型，单标签、多标签
     */
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;
    /**
     * 主题系统标识
     */
    private int topicSysFlag = 0;
    /**
     * 主题是否有序
     */
    private boolean order = false;

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    /**
     * 对主题配置进行编码成字节数组
     */
    public String encode() {
        StringBuilder sb = new StringBuilder();
        //主题名称
        sb.append(this.topicName);
        //分隔符
        sb.append(SEPARATOR);
        //读队列数目
        sb.append(this.readQueueNums);
        //分隔符
        sb.append(SEPARATOR);
        //写队列数目
        sb.append(this.writeQueueNums);
        //分隔符
        sb.append(SEPARATOR);
        //topic权限
        sb.append(this.perm);
        //分隔符
        sb.append(SEPARATOR);
        //主题过滤类型，单标签、多标签
        sb.append(this.topicFilterType);

        return sb.toString();
    }

    /**
     * 解码，将字符串转成主题的配置信息
     */
    public boolean decode(final String in) {
        //将字符串通过分隔符进行分割
        String[] strs = in.split(SEPARATOR);
        if (strs != null && strs.length == 5) {
            //主题名称
            this.topicName = strs[0];
            //读队列数目
            this.readQueueNums = Integer.parseInt(strs[1]);
            //写队列数目
            this.writeQueueNums = Integer.parseInt(strs[2]);
            //topic权限
            this.perm = Integer.parseInt(strs[3]);
            //主题过滤类型，单标签、多标签
            this.topicFilterType = TopicFilterType.valueOf(strs[4]);
            //解码成功
            return true;
        }
        //解码失败
        return false;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public TopicFilterType getTopicFilterType() {
        return topicFilterType;
    }

    public void setTopicFilterType(TopicFilterType topicFilterType) {
        this.topicFilterType = topicFilterType;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean isOrder) {
        this.order = isOrder;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final TopicConfig that = (TopicConfig) o;

        if (readQueueNums != that.readQueueNums)
            return false;
        if (writeQueueNums != that.writeQueueNums)
            return false;
        if (perm != that.perm)
            return false;
        if (topicSysFlag != that.topicSysFlag)
            return false;
        if (order != that.order)
            return false;
        if (topicName != null ? !topicName.equals(that.topicName) : that.topicName != null)
            return false;
        return topicFilterType == that.topicFilterType;

    }

    @Override
    public int hashCode() {
        int result = topicName != null ? topicName.hashCode() : 0;
        result = 31 * result + readQueueNums;
        result = 31 * result + writeQueueNums;
        result = 31 * result + perm;
        result = 31 * result + (topicFilterType != null ? topicFilterType.hashCode() : 0);
        result = 31 * result + topicSysFlag;
        result = 31 * result + (order ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TopicConfig [topicName=" + topicName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + PermName.perm2String(perm)
            + ", topicFilterType=" + topicFilterType + ", topicSysFlag=" + topicSysFlag + ", order="
            + order + "]";
    }
}
