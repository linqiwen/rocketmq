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

package org.apache.rocketmq.filter;

import java.util.HashMap;
import java.util.Map;

/**
 * 过滤器工厂：支持其他过滤器注册
 */
public class FilterFactory {


    public static final FilterFactory INSTANCE = new FilterFactory();

    protected static final Map<String, FilterSpi> FILTER_SPI_HOLDER = new HashMap<String, FilterSpi>(4);

    static {
        //注册过滤器
        FilterFactory.INSTANCE.register(new SqlFilter());
    }

    /**
     * 注册过滤器.
     * <br>
     * Note:
     * <li>1. 注册的过滤器将在代理服务器中使用，所以要注意它的可靠性和性能.</li>
     */
    public void register(FilterSpi filterSpi) {
        if (FILTER_SPI_HOLDER.containsKey(filterSpi.ofType())) {
            throw new IllegalArgumentException(String.format("Filter spi type(%s) already exist!", filterSpi.ofType()));
        }

        FILTER_SPI_HOLDER.put(filterSpi.ofType(), filterSpi);
    }

    /**
     * 注销过滤器
     *
     * @param type 过滤器类型
     * @return 过滤器
     */
    public FilterSpi unRegister(String type) {
        return FILTER_SPI_HOLDER.remove(type);
    }

    /**
     * 获取已注册的筛选器，如果不存在，则为空
     *
     * @param type 过滤器类型
     * @return 过滤器
     */
    public FilterSpi get(String type) {
        return FILTER_SPI_HOLDER.get(type);
    }

}
