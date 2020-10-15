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

import java.util.List;

/**
 * 对两个对象进行比较的过滤器
 * <p>
 * 这个类取自ActiveMQ org.apache.activemq.filter.ComparisonExpression,
 * but:
 * 1. 消除LIKE表达式, 和相关方法;
 * 2. 提取一个新的返回值为int的方法 __compare;
 * 3. 当创建between表达式时，检查左值是否小于或等于右值；
 * 4. 对于字符串类型值(不能转换为数字), 只支持相等或不等比较
 * </p>
 */
public abstract class ComparisonExpression extends BinaryExpression implements BooleanExpression {

    /**
     * 是否转换字符串表达式本地线程存储
     */
    public static final ThreadLocal<Boolean> CONVERT_STRING_EXPRESSIONS = new ThreadLocal<Boolean>();

    /**
     * 是否转换字符串表达式
     */
    boolean convertStringExpressions = false;

    /**
     * @param left 左表达式
     * @param right 右表达式
     */
    public ComparisonExpression(Expression left, Expression right) {
        super(left, right);
        convertStringExpressions = CONVERT_STRING_EXPRESSIONS.get() != null;
    }

    /**
     * 创建Between表达式
     */
    public static BooleanExpression createBetween(Expression value, Expression left, Expression right) {
        // 检查左右表达式是否常量表达式，如果是常量表达式，需检查左值是否小于或等于右值
        if (left instanceof ConstantExpression && right instanceof ConstantExpression) {
            //左常量表达式值
            Object lv = ((ConstantExpression) left).getValue();
            //右常量表达式值
            Object rv = ((ConstantExpression) right).getValue();
            if (lv == null || rv == null) {
                //只要一个为空就抛异常
                throw new RuntimeException("Illegal values of between, values can not be null!");
            }
            //判断左右表达式是否可对比
            if (lv instanceof Comparable && rv instanceof Comparable) {
                int ret = __compare((Comparable) rv, (Comparable) lv, true);
                if (ret < 0)
                    //如果左表达式值没有小于右表达式值抛异常
                    throw new RuntimeException(
                        String.format("Illegal values of between, left value(%s) must less than or equal to right value(%s)", lv, rv)
                    );
            }
        }

        return LogicExpression.createAND(createGreaterThanEqual(value, left), createLessThanEqual(value, right));
    }

    public static BooleanExpression createNotBetween(Expression value, Expression left, Expression right) {
        return LogicExpression.createOR(createLessThan(value, left), createGreaterThan(value, right));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static BooleanExpression createInFilter(Expression left, List elements) {

        if (!(left instanceof PropertyExpression)) {
            throw new RuntimeException("Expected a property for In expression, got: " + left);
        }
        return UnaryExpression.createInExpression((PropertyExpression) left, elements, false);

    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static BooleanExpression createNotInFilter(Expression left, List elements) {

        if (!(left instanceof PropertyExpression)) {
            throw new RuntimeException("Expected a property for In expression, got: " + left);
        }
        return UnaryExpression.createInExpression((PropertyExpression) left, elements, true);

    }

    public static BooleanExpression createIsNull(Expression left) {
        return doCreateEqual(left, ConstantExpression.NULL);
    }

    public static BooleanExpression createIsNotNull(Expression left) {
        return UnaryExpression.createNOT(doCreateEqual(left, ConstantExpression.NULL));
    }

    public static BooleanExpression createNotEqual(Expression left, Expression right) {
        return UnaryExpression.createNOT(createEqual(left, right));
    }

    public static BooleanExpression createEqual(Expression left, Expression right) {
        checkEqualOperand(left);
        checkEqualOperand(right);
        checkEqualOperandCompatability(left, right);
        return doCreateEqual(left, right);
    }

    @SuppressWarnings({"rawtypes"})
    private static BooleanExpression doCreateEqual(Expression left, Expression right) {
        return new ComparisonExpression(left, right) {

            public Object evaluate(EvaluationContext context) throws Exception {
                Object lv = left.evaluate(context);
                Object rv = right.evaluate(context);

                // If one of the values is null
                if (lv == null ^ rv == null) {
                    if (lv == null) {
                        return null;
                    }
                    return Boolean.FALSE;
                }
                if (lv == rv || lv.equals(rv)) {
                    return Boolean.TRUE;
                }
                if (lv instanceof Comparable && rv instanceof Comparable) {
                    return compare((Comparable) lv, (Comparable) rv);
                }
                return Boolean.FALSE;
            }

            protected boolean asBoolean(int answer) {
                return answer == 0;
            }

            public String getExpressionSymbol() {
                return "==";
            }
        };
    }

    public static BooleanExpression createGreaterThan(final Expression left, final Expression right) {
        checkLessThanOperand(left);
        checkLessThanOperand(right);
        return new ComparisonExpression(left, right) {
            protected boolean asBoolean(int answer) {
                return answer > 0;
            }

            public String getExpressionSymbol() {
                return ">";
            }
        };
    }

    public static BooleanExpression createGreaterThanEqual(final Expression left, final Expression right) {
        checkLessThanOperand(left);
        checkLessThanOperand(right);
        return new ComparisonExpression(left, right) {
            protected boolean asBoolean(int answer) {
                return answer >= 0;
            }

            public String getExpressionSymbol() {
                return ">=";
            }
        };
    }

    public static BooleanExpression createLessThan(final Expression left, final Expression right) {
        checkLessThanOperand(left);
        checkLessThanOperand(right);
        return new ComparisonExpression(left, right) {

            protected boolean asBoolean(int answer) {
                return answer < 0;
            }

            public String getExpressionSymbol() {
                return "<";
            }

        };
    }

    public static BooleanExpression createLessThanEqual(final Expression left, final Expression right) {
        checkLessThanOperand(left);
        checkLessThanOperand(right);
        return new ComparisonExpression(left, right) {

            protected boolean asBoolean(int answer) {
                return answer <= 0;
            }

            public String getExpressionSymbol() {
                return "<=";
            }
        };
    }

    /**
     * Only Numeric expressions can be used in >, >=, < or <= expressions.s
     */
    public static void checkLessThanOperand(Expression expr) {
        if (expr instanceof ConstantExpression) {
            Object value = ((ConstantExpression) expr).getValue();
            if (value instanceof Number) {
                return;
            }

            // Else it's boolean or a String..
            throw new RuntimeException("Value '" + expr + "' cannot be compared.");
        }
        if (expr instanceof BooleanExpression) {
            throw new RuntimeException("Value '" + expr + "' cannot be compared.");
        }
    }

    /**
     * Validates that the expression can be used in == or <> expression. Cannot
     * not be NULL TRUE or FALSE litterals.
     */
    public static void checkEqualOperand(Expression expr) {
        if (expr instanceof ConstantExpression) {
            Object value = ((ConstantExpression) expr).getValue();
            if (value == null) {
                throw new RuntimeException("'" + expr + "' cannot be compared.");
            }
        }
    }

    /**
     * @param left
     * @param right
     */
    private static void checkEqualOperandCompatability(Expression left, Expression right) {
        if (left instanceof ConstantExpression && right instanceof ConstantExpression) {
            if (left instanceof BooleanExpression && !(right instanceof BooleanExpression)) {
                throw new RuntimeException("'" + left + "' cannot be compared with '" + right + "'");
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Object evaluate(EvaluationContext context) throws Exception {
        Comparable<Comparable> lv = (Comparable) left.evaluate(context);
        if (lv == null) {
            return null;
        }
        Comparable rv = (Comparable) right.evaluate(context);
        if (rv == null) {
            return null;
        }
        if (getExpressionSymbol().equals(">=") || getExpressionSymbol().equals(">")
            || getExpressionSymbol().equals("<") || getExpressionSymbol().equals("<=")) {
            Class<? extends Comparable> lc = lv.getClass();
            Class<? extends Comparable> rc = rv.getClass();
            if (lc == rc && lc == String.class) {
                // Compare String is illegal
                // first try to convert to double
                try {
                    Comparable lvC = Double.valueOf((String) (Comparable) lv);
                    Comparable rvC = Double.valueOf((String) rv);

                    return compare(lvC, rvC);
                } catch (Exception e) {
                    throw new RuntimeException("It's illegal to compare string by '>=', '>', '<', '<='. lv=" + lv + ", rv=" + rv, e);
                }
            }
        }
        return compare(lv, rv);
    }

    /**
     * 比较两个值
     *
     * @param lv 比较表达式左边的值
     * @param rv 比较表达式右边的值
     * @param convertStringExpressions 左表达式或右表达式为String类型，是否需要转成特定类型
     * @return {@code 0}相等，{@code 1}lv大于rv，{@code -1}lv小于rv
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected static int __compare(Comparable lv, Comparable rv, boolean convertStringExpressions) {
        //获取左值的class类型
        Class<? extends Comparable> lc = lv.getClass();
        //获取右值的class类型
        Class<? extends Comparable> rc = rv.getClass();
        // If the the objects are not of the same type,
        // try to convert up to allow the comparison.
        if (lc != rc) {
            try {
                //如果左表达式值类型是Boolean类型
                if (lc == Boolean.class) {
                    //convertStringExpressions为true，并且右表达式值类型String，将String转成boolean类型
                    if (convertStringExpressions && rc == String.class) {
                        lv = Boolean.valueOf((String) lv).booleanValue();
                    } else {
                        //否则直接返回-1
                        return -1;
                    }
                } else if (lc == Byte.class) {//如果左表达式值类型是Byte类型
                    //右表达式值类型Short类型
                    if (rc == Short.class) {
                        //将左表达式值Byte转成Short类型值
                        lv = Short.valueOf(((Number) lv).shortValue());
                    } else if (rc == Integer.class) {//右表达式值类型是Integer类型
                        //将左表达式值Byte转成Integer类型值
                        lv = Integer.valueOf(((Number) lv).intValue());
                    } else if (rc == Long.class) {//右表达式值类型是Long类型
                        //将左表达式值Byte转成Long类型值
                        lv = Long.valueOf(((Number) lv).longValue());
                    } else if (rc == Float.class) {//右表达式值类型是Float类型
                        //将左表达式值Byte转成Float类型值
                        lv = new Float(((Number) lv).floatValue());
                    } else if (rc == Double.class) {//右表达式值类型是Double类型
                        //将左表达式值Byte转成Double类型值
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {//如果右表达式类型是String类型，并且convertStringExpressions为true
                        //将有表达式值Strung转成Byte类型
                        rv = Byte.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Short.class) {//如果左表达式值类型是short类型
                    if (rc == Integer.class) {//右表达式值类型是Integer类型
                        //将左表达式值Short转成Integer类型值
                        lv = Integer.valueOf(((Number) lv).intValue());
                    } else if (rc == Long.class) {//右表达式值类型是Long类型
                        //将左表达式值Short转成Long类型值
                        lv = Long.valueOf(((Number) lv).longValue());
                    } else if (rc == Float.class) {//右表达式值类型是Float类型
                        //将左表达式值Short转成Float类型值
                        lv = new Float(((Number) lv).floatValue());
                    } else if (rc == Double.class) {//右表达式值类型是Double类型
                        //将左表达式值Short转成Double类型值
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {//如果右表达式类型是String类型，并且convertStringExpressions为true
                        //将有表达式值Strung转成Short类型
                        rv = Short.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Integer.class) {//如果左表达式值类型是Integer类型
                    if (rc == Long.class) {//右表达式值类型是Long类型
                        //将左表达式值Integer转成Long类型值
                        lv = Long.valueOf(((Number) lv).longValue());
                    } else if (rc == Float.class) {//右表达式值类型是Float类型
                        //将左表达式值Integer转成Float类型值
                        lv = new Float(((Number) lv).floatValue());
                    } else if (rc == Double.class) {//右表达式值类型是Double类型
                        //将左表达式值Integer转成Double类型值
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {//如果右表达式类型是String类型，并且convertStringExpressions为true
                        //将有表达式值Strung转成Integer类型
                        rv = Integer.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Long.class) {//如果左表达式值类型是Long类型
                    if (rc == Integer.class) {//右表达式值类型是Integer类型
                        //将右表达式值Integer转成Long类型值
                        rv = Long.valueOf(((Number) rv).longValue());
                    } else if (rc == Float.class) {//右表达式值类型是Float类型
                        //将左表达式值Long转成Float类型
                        lv = new Float(((Number) lv).floatValue());
                    } else if (rc == Double.class) {//右表达式值类型是Double类型
                        //将左表达式值Long转成Double类型
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {//如果右表达式类型是String类型，并且convertStringExpressions为true
                        //将有表达式值Strung转成Long类型
                        rv = Long.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Float.class) {//如果左表达式值类型是Float类型
                    if (rc == Integer.class) {//右表达式值类型是Integer类型
                        //将右表达式值Integer转成Float类型值
                        rv = new Float(((Number) rv).floatValue());
                    } else if (rc == Long.class) {//右表达式值类型是Long类型
                        //将右表达式值Long转成Float类型值
                        rv = new Float(((Number) rv).floatValue());
                    } else if (rc == Double.class) {//右表达式值类型是Double类型
                        //将左表达式值Long转成Double类型
                        lv = new Double(((Number) lv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {//如果右表达式类型是String类型，并且convertStringExpressions为true
                        //将有表达式值Strung转成Float类型
                        rv = Float.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (lc == Double.class) {//如果左表达式值类型是Double类型
                    if (rc == Integer.class) {//右表达式值类型是Integer类型
                        //将右表达式值Integer转成Double类型值
                        rv = new Double(((Number) rv).doubleValue());
                    } else if (rc == Long.class) {//右表达式值类型是Long类型
                        //将右表达式值Long转成Double类型值
                        rv = new Double(((Number) rv).doubleValue());
                    } else if (rc == Float.class) {//右表达式值类型是Float类型
                        //将右表达式值Float转成Double类型值
                        rv = new Float(((Number) rv).doubleValue());
                    } else if (convertStringExpressions && rc == String.class) {//如果右表达式类型是String类型，并且convertStringExpressions为true
                        //将右表达式值Strung转成Double类型
                        rv = Double.valueOf((String) rv);
                    } else {
                        return -1;
                    }
                } else if (convertStringExpressions && lc == String.class) {//如果左表达式类型是String类型，并且convertStringExpressions为true
                    if (rc == Boolean.class) {//右表达式值类型是Boolean类型
                        //将左表达式值String转成Boolena类型
                        lv = Boolean.valueOf((String) lv);
                    } else if (rc == Byte.class) {//右表达式值类型是Byte类型
                        //将左表达式值String转成Byte类型
                        lv = Byte.valueOf((String) lv);
                    } else if (rc == Short.class) {//右表达式值类型是Short类型
                        //将左表达式值String转成Short类型
                        lv = Short.valueOf((String) lv);
                    } else if (rc == Integer.class) {//右表达式值类型是Integer类型
                        //将左表达式值String转成Integer类型
                        lv = Integer.valueOf((String) lv);
                    } else if (rc == Long.class) {//右表达式值类型是Long类型
                        //将左表达式值String转成Long类型
                        lv = Long.valueOf((String) lv);
                    } else if (rc == Float.class) {//右表达式值类型是Float类型
                        //将左表达式值String转成Float类型
                        lv = Float.valueOf((String) lv);
                    } else if (rc == Double.class) {//右表达式值类型是Double类型
                        //将左表达式值String转成Double类型
                        lv = Double.valueOf((String) lv);
                    } else {
                        return -1;
                    }
                } else {
                    return -1;
                }
            } catch (NumberFormatException e) {
                throw new RuntimeException(e);
            }
        }
        return lv.compareTo(rv);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Boolean compare(Comparable lv, Comparable rv) {
        return asBoolean(__compare(lv, rv, convertStringExpressions)) ? Boolean.TRUE : Boolean.FALSE;
    }

    protected abstract boolean asBoolean(int answer);

    public boolean matches(EvaluationContext context) throws Exception {
        Object object = evaluate(context);
        return object != null && object == Boolean.TRUE;
    }

}
