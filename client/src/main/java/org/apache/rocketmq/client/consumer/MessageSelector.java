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

package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.common.filter.ExpressionType;

/**
 * 消息选择器：选择服务器上的消息
 * <p>
 * 目前, 支持:
 * <li>Tag: {@link org.apache.rocketmq.common.filter.ExpressionType#TAG}
 * </li>
 * <li>SQL92: {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}
 * </li>
 * </p>
 */
public class MessageSelector {

    /**
     * 表达式类型
     * @see org.apache.rocketmq.common.filter.ExpressionType
     */
    private String type;

    /**
     * 表达内容.
     */
    private String expression;

    private MessageSelector(String type, String expression) {
        this.type = type;
        this.expression = expression;
    }

    /**
     * 使用SLQ92选择消息.
     *
     * @param sql 如果为null或为空，将被视为选择所有消息。
     */
    public static MessageSelector bySql(String sql) {
        return new MessageSelector(ExpressionType.SQL92, sql);
    }

    /**
     * 使用标签选择消息
     *
     * @param tag if null or empty or "*", will be treated as select all message.
     */
    public static MessageSelector byTag(String tag) {
        return new MessageSelector(ExpressionType.TAG, tag);
    }

    public String getExpressionType() {
        return type;
    }

    public String getExpression() {
        return expression;
    }
}
