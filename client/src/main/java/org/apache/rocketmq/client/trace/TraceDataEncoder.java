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
package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.common.message.MessageType;

import java.util.ArrayList;
import java.util.List;

/**
 * 跟踪数据的编码/解码
 */
public class TraceDataEncoder {

    /**
     * 从跟踪数据字符串解析traceContext列表
     *
     * @param traceData 跟踪数据字符串
     * @return traceContext列表
     */
    public static List<TraceContext> decoderFromTraceDataString(String traceData) {
        List<TraceContext> resList = new ArrayList<TraceContext>();
        if (traceData == null || traceData.length() <= 0) {
            return resList;
        }
        String[] contextList = traceData.split(String.valueOf(TraceConstants.FIELD_SPLITOR));
        for (String context : contextList) {
            String[] line = context.split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
            if (line[0].equals(TraceType.Pub.name())) {
                TraceContext pubContext = new TraceContext();
                pubContext.setTraceType(TraceType.Pub);
                pubContext.setTimeStamp(Long.parseLong(line[1]));
                pubContext.setRegionId(line[2]);
                pubContext.setGroupName(line[3]);
                TraceBean bean = new TraceBean();
                bean.setTopic(line[4]);
                bean.setMsgId(line[5]);
                bean.setTags(line[6]);
                bean.setKeys(line[7]);
                bean.setStoreHost(line[8]);
                bean.setBodyLength(Integer.parseInt(line[9]));
                pubContext.setCostTime(Integer.parseInt(line[10]));
                bean.setMsgType(MessageType.values()[Integer.parseInt(line[11])]);

                if (line.length == 13) {
                    pubContext.setSuccess(Boolean.parseBoolean(line[12]));
                } else if (line.length == 14) {
                    bean.setOffsetMsgId(line[12]);
                    pubContext.setSuccess(Boolean.parseBoolean(line[13]));
                }
                pubContext.setTraceBeans(new ArrayList<TraceBean>(1));
                pubContext.getTraceBeans().add(bean);
                resList.add(pubContext);
            } else if (line[0].equals(TraceType.SubBefore.name())) {
                TraceContext subBeforeContext = new TraceContext();
                subBeforeContext.setTraceType(TraceType.SubBefore);
                subBeforeContext.setTimeStamp(Long.parseLong(line[1]));
                subBeforeContext.setRegionId(line[2]);
                subBeforeContext.setGroupName(line[3]);
                subBeforeContext.setRequestId(line[4]);
                TraceBean bean = new TraceBean();
                bean.setMsgId(line[5]);
                bean.setRetryTimes(Integer.parseInt(line[6]));
                bean.setKeys(line[7]);
                subBeforeContext.setTraceBeans(new ArrayList<TraceBean>(1));
                subBeforeContext.getTraceBeans().add(bean);
                resList.add(subBeforeContext);
            } else if (line[0].equals(TraceType.SubAfter.name())) {
                TraceContext subAfterContext = new TraceContext();
                subAfterContext.setTraceType(TraceType.SubAfter);
                subAfterContext.setRequestId(line[1]);
                TraceBean bean = new TraceBean();
                bean.setMsgId(line[2]);
                bean.setKeys(line[5]);
                subAfterContext.setTraceBeans(new ArrayList<TraceBean>(1));
                subAfterContext.getTraceBeans().add(bean);
                subAfterContext.setCostTime(Integer.parseInt(line[3]));
                subAfterContext.setSuccess(Boolean.parseBoolean(line[4]));
                if (line.length >= 7) {
                    // add the context type
                    subAfterContext.setContextCode(Integer.parseInt(line[6]));
                }
                resList.add(subAfterContext);
            }
        }
        return resList;
    }

    /**
     * 编码跟踪上下文数据和keySet成字符串
     *
     * @param ctx 跟踪上下文
     * @return 跟踪传输bean
     */
    public static TraceTransferBean encoderFromContextBean(TraceContext ctx) {
        if (ctx == null) {
            return null;
        }
        //构建转让的消息跟踪实体bean的内容
        TraceTransferBean transferBean = new TraceTransferBean();
        StringBuilder sb = new StringBuilder(256);
        switch (ctx.getTraceType()) {
            case Pub: {
                TraceBean bean = ctx.getTraceBeans().get(0);
                //附加上下文的内容和跟踪Bean到传输数据中
                //添加跟踪类型
                sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加当前时间戳
                    .append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加broker所在的区域ID
                    .append(ctx.getRegionId()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加生产组的名称
                    .append(ctx.getGroupName()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加消息主题
                    .append(bean.getTopic()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加消息的msgId
                    .append(bean.getMsgId()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加消息的tags
                    .append(bean.getTags()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加消息的keys
                    .append(bean.getKeys()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加存储该消息的Broker服务器IP
                    .append(bean.getStoreHost()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加消息的内容长度
                    .append(bean.getBodyLength()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加消息的耗费时间
                    .append(ctx.getCostTime()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加消息类型
                    .append(bean.getMsgType().ordinal()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加消息偏移量
                    .append(bean.getOffsetMsgId()).append(TraceConstants.CONTENT_SPLITOR)//
                    //添加是否消费成功
                    .append(ctx.isSuccess()).append(TraceConstants.FIELD_SPLITOR);
            }
            break;
            case SubBefore: {
                for (TraceBean bean : ctx.getTraceBeans()) {
                    //添加跟踪类型
                    sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加当前时间戳
                        .append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加broker所在的区域ID
                        .append(ctx.getRegionId()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消费组的名称
                        .append(ctx.getGroupName()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消费端的请求id
                        .append(ctx.getRequestId()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消息的msgId
                        .append(bean.getMsgId()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消费者的重试次数
                        .append(bean.getRetryTimes()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消息的keys
                        .append(bean.getKeys()).append(TraceConstants.FIELD_SPLITOR);//
                }
            }
            break;
            case SubAfter: {
                for (TraceBean bean : ctx.getTraceBeans()) {
                    //添加跟踪类型
                    sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消费端的请求id
                        .append(ctx.getRequestId()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消息的msgId
                        .append(bean.getMsgId()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加耗费的时间
                        .append(ctx.getCostTime()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消费是否成功
                        .append(ctx.isSuccess()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消息的keys
                        .append(bean.getKeys()).append(TraceConstants.CONTENT_SPLITOR)//
                        //添加消息消费的状态码
                        .append(ctx.getContextCode()).append(TraceConstants.FIELD_SPLITOR);
                }
            }
            break;
            default:
        }
        transferBean.setTransData(sb.toString());
        for (TraceBean bean : ctx.getTraceBeans()) {

            transferBean.getTransKey().add(bean.getMsgId());
            if (bean.getKeys() != null && bean.getKeys().length() > 0) {
                transferBean.getTransKey().add(bean.getKeys());
            }
        }
        return transferBean;
    }
}
