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
 * 对两个表达式值执行操作的表达式
 * <p>
 * 这个类取自ActiveMQ org.apache.activemq.filter.BinaryExpression,
 * </p>
 */
public abstract class BinaryExpression implements Expression {
    /**
     * 左表达式
     */
    protected Expression left;
    /**
     * 右表达式
     */
    protected Expression right;

    public BinaryExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        return "(" + left.toString() + " " + getExpressionSymbol() + " " + right.toString() + ")";
    }

    /**
     * @see Object#hashCode()
     */
    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * @see Object#equals(Object)
     */
    public boolean equals(Object o) {

        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return toString().equals(o.toString());

    }

    /**
     * 返回表示此二进制表达式的符号。例如，加法用"+"表示
     *
     * @return 表达式符合
     */
    public abstract String getExpressionSymbol();

    /**
     * 设置左表达式
     *
     * @param expression 表达式
     */
    public void setRight(Expression expression) {
        right = expression;
    }

    /**
     * @param expression
     */
    public void setLeft(Expression expression) {
        left = expression;
    }

}
