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
 * 表示常量表达式
 * <p>
 * 这个类是取自ActiveMQ org.apache.activemq.filter.ConstantExpression,
 * but:
 * 1. 对于long类型常量，由java long类型绑定的范围;
 * 2. 对于float类型常量，由java Double类型绑定的范围;
 * 3. 删除十六进制和八进制表达式;
 * 4. 添加now表达式以支持获取当前时间.
 * </p>
 */
public class ConstantExpression implements Expression {

    static class BooleanConstantExpression extends ConstantExpression implements BooleanExpression {
        public BooleanConstantExpression(Object value) {
            super(value);
        }

        public boolean matches(EvaluationContext context) throws Exception {
            Object object = evaluate(context);
            return object != null && object == Boolean.TRUE;
        }
    }

    public static final BooleanConstantExpression NULL = new BooleanConstantExpression(null);
    public static final BooleanConstantExpression TRUE = new BooleanConstantExpression(Boolean.TRUE);
    public static final BooleanConstantExpression FALSE = new BooleanConstantExpression(Boolean.FALSE);

    private Object value;

    public ConstantExpression(Object value) {
        this.value = value;
    }

    public static ConstantExpression createFromDecimal(String text) {

        // Strip off the 'l' or 'L' if needed.
        if (text.endsWith("l") || text.endsWith("L")) {
            text = text.substring(0, text.length() - 1);
        }

        // only support Long.MIN_VALUE ~ Long.MAX_VALUE
        Number value = new Long(text);
//        try {
//            value = new Long(text);
//        } catch (NumberFormatException e) {
//            // The number may be too big to fit in a long.
//            value = new BigDecimal(text);
//        }

        long l = value.longValue();
        if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
            value = Integer.valueOf(value.intValue());
        }
        return new ConstantExpression(value);
    }

    /**
     * 创建常量的float表达式
     *
     * @param text 表达式值
     * @return Double的常量表达式
     * @throws RuntimeException 表达式值如果不在[Double.MIN_VALUE, Double.MAX_VALUE]范围
     */
    public static ConstantExpression createFloat(String text) {
        Double value = new Double(text);
        if (value > Double.MAX_VALUE) {
            throw new RuntimeException(text + " is greater than " + Double.MAX_VALUE);
        }
        if (value < Double.MIN_VALUE) {
            throw new RuntimeException(text + " is less than " + Double.MIN_VALUE);
        }
        return new ConstantExpression(value);
    }

    public static ConstantExpression createNow() {
        return new NowExpression();
    }

    public Object evaluate(EvaluationContext context) throws Exception {
        return value;
    }

    public Object getValue() {
        return value;
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        Object value = getValue();
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue() ? "TRUE" : "FALSE";
        }
        if (value instanceof String) {
            return encodeString((String) value);
        }
        return value.toString();
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
     * Encodes the value of string so that it looks like it would look like when
     * it was provided in a selector.
     */
    public static String encodeString(String s) {
        StringBuffer b = new StringBuffer();
        b.append('\'');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                b.append(c);
            }
            b.append(c);
        }
        b.append('\'');
        return b.toString();
    }

}
