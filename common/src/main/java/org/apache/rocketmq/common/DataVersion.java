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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 数据版本
 */
public class DataVersion extends RemotingSerializable {
    /**
     * 时间戳
     */
    private long timestamp = System.currentTimeMillis();
    /**
     * 计数器
     */
    private AtomicLong counter = new AtomicLong(0);

    /**
     * 分配新的数据版本
     */
    public void assignNewOne(final DataVersion dataVersion) {
        //重新设置时间戳
        this.timestamp = dataVersion.timestamp;
        //重新设置计数器
        this.counter.set(dataVersion.counter.get());
    }

    /**
     * 下一个版本
     */
    public void nextVersion() {
        //时间戳
        this.timestamp = System.currentTimeMillis();
        //计数器加1
        this.counter.incrementAndGet();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public AtomicLong getCounter() {
        return counter;
    }

    public void setCounter(AtomicLong counter) {
        this.counter = counter;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final DataVersion that = (DataVersion) o;

        if (timestamp != that.timestamp) {
            return false;
        }

        if (counter != null && that.counter != null) {
            return counter.longValue() == that.counter.longValue();
        }

        return (null == counter) && (null == that.counter);
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        if (null != counter) {
            long l = counter.get();
            result = 31 * result + (int) (l ^ (l >>> 32));
        }
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DataVersion[");
        sb.append("timestamp=").append(timestamp);
        sb.append(", counter=").append(counter);
        sb.append(']');
        return sb.toString();
    }
}
