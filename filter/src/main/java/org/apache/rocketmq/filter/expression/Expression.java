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

package org.apache.rocketmq.filter.expression;

/**
 * 表达式接口
 * <p>
 * 这个类取自ActiveMQ org.apache.activemq.filter.Expression,
 * 但参数更改为接口
 * </p>
 *
 * @see org.apache.rocketmq.filter.expression.EvaluationContext
 */
public interface Expression {

    /**
     * 使用上下文计算表达式结果
     *
     * @param context 计算的上下文
     * @return 此表达式的值
     */
    Object evaluate(EvaluationContext context) throws Exception;
}
