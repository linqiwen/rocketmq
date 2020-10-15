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

import java.util.Map;

/**
 * 求值表达式的上下文
 *
 * 和org.apache.activemq.filter.MessageEvaluationContext比较, 这只是个接口
 */
public interface EvaluationContext {

    /**
     * 根据名称从上下文获取值
     *
     * @param name 名称
     * @return 名称所对应的值
     */
    Object get(String name);

    /**
     * 上下文变量
     *
     * @return 上下文变量
     */
    Map<String, Object> keyValues();
}
