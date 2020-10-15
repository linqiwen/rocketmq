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
package org.apache.rocketmq.common.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.rocketmq.common.MixAll;

/**
 * 批量消息
 */
public class MessageBatch extends Message implements Iterable<Message> {

    private static final long serialVersionUID = 621335151046335557L;
    /**
     * 消息列表
     */
    private final List<Message> messages;

    private MessageBatch(List<Message> messages) {
        this.messages = messages;
    }

    /**
     * 编码
     */
    public byte[] encode() {
        return MessageDecoder.encodeMessages(messages);
    }

    /**
     * 消息迭代器
     */
    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    /**
     * 根据消息列表生成批量消息
     *
     * @param messages 消息列表
     */
    public static MessageBatch generateFromList(Collection<Message> messages) {
        //消息列表不能为空
        assert messages != null;
        //消息列表长度大于0
        assert messages.size() > 0;
        List<Message> messageList = new ArrayList<Message>(messages.size());
        Message first = null;
        for (Message message : messages) {
            //延迟不支持批量的消息
            if (message.getDelayTimeLevel() > 0) {
                throw new UnsupportedOperationException("TimeDelayLevel in not supported for batching");
            }
            //重试组不支持批量的处理
            if (message.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                throw new UnsupportedOperationException("Retry Group is not supported for batching");
            }
            if (first == null) {
                //第一条进行处理的消息
                first = message;
            } else {
                //一个批处理中的消息主题应该相同
                if (!first.getTopic().equals(message.getTopic())) {
                    throw new UnsupportedOperationException("The topic of the messages in one batch should be the same");
                }
                //一个批处理中的消息的waitStoreMsgOK应该相同
                if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK()) {
                    throw new UnsupportedOperationException("The waitStoreMsgOK of the messages in one batch should the same");
                }
            }
            //将消息加到messageList中
            messageList.add(message);
        }
        //创建批处理
        MessageBatch messageBatch = new MessageBatch(messageList);
        //设置topic
        messageBatch.setTopic(first.getTopic());
        //设置是否等待消息被存储成功
        messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());
        //返回批处理
        return messageBatch;
    }

}
