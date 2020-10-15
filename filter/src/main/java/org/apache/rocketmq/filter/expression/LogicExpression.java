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
 * 对两个对象进行比较的过滤器
 * <p>
 * 这个类来自ActiveMQ org.apache.activemq.filter.LogicExpression,
 * </p>
 */
public abstract class LogicExpression extends BinaryExpression implements BooleanExpression {

    /**
     * @param left 左表达式
     * @param right 右表达式
     */
    public LogicExpression(BooleanExpression left, BooleanExpression right) {
        super(left, right);
    }

    /**
     * 创建短路或，一个为true就为true
     *
     * @param lvalue 短路或的左边表达式
     * @param rvalue 短路或的右边表达式
     * @return 短路或表达式
     */
    public static BooleanExpression createOR(BooleanExpression lvalue, BooleanExpression rvalue) {
        return new LogicExpression(lvalue, rvalue) {

            public Object evaluate(EvaluationContext context) throws Exception {

                //左边表达式结果
                Boolean lv = (Boolean) left.evaluate(context);
                //左边表达式的值为true，直接返回true
                if (lv != null && lv.booleanValue()) {
                    return Boolean.TRUE;
                }
                //右边表达式的值为true，也返回true
                Boolean rv = (Boolean) right.evaluate(context);
                if (rv != null && rv.booleanValue()) {
                    return Boolean.TRUE;
                }
                //如果两个表达式其中一个为空，直接返回null
                if (lv == null || rv == null) {
                    return null;
                }
                //如果两个为false，返回false
                return Boolean.FALSE;
            }

            public String getExpressionSymbol() {
                return "||";
            }
        };
    }

    /**
     * 创建短路与表达式
     *
     * @param lvalue 短路与的左边表达式
     * @param rvalue 短路与的右边表达式
     * @return 短路与表达式
     */
    public static BooleanExpression createAND(BooleanExpression lvalue, BooleanExpression rvalue) {
        return new LogicExpression(lvalue, rvalue) {

            public Object evaluate(EvaluationContext context) throws Exception {

                //左边表达式结果
                Boolean lv = (Boolean) left.evaluate(context);

                //左边表达式的值为false，直接返回false
                if (lv != null && !lv.booleanValue()) {
                    return Boolean.FALSE;
                }
                //右边表达式结果
                Boolean rv = (Boolean) right.evaluate(context);
                //右边表达式的值为false，直接返回false
                if (rv != null && !rv.booleanValue()) {
                    return Boolean.FALSE;
                }
                //如果两个表达式其中一个为空，直接返回null
                if (lv == null || rv == null) {
                    return null;
                }
                //如果两个为true，返回true
                return Boolean.TRUE;
            }

            /**
             * 获取表达式符号
             */
            public String getExpressionSymbol() {
                return "&&";
            }
        };
    }

    public abstract Object evaluate(EvaluationContext context) throws Exception;

    public boolean matches(EvaluationContext context) throws Exception {
        Object object = evaluate(context);
        return object != null && object == Boolean.TRUE;
    }

}
