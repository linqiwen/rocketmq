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
package org.apache.rocketmq.common;

import java.io.IOException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 配置管理器
 */
public abstract class ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 编码
     */
    public abstract String encode();

    public boolean load() {
        String fileName = null;
        try {
            //配置文件路径
            fileName = this.configFilePath();
            //获取文件内容
            String jsonString = MixAll.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                //如果文件内容为空，读取备份文件
                return this.loadBak();
            } else {
                //对文件进行解码
                this.decode(jsonString);
                //打印日志
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            //打印日志
            log.error("load " + fileName + " failed, and try to load backup file", e);
            //读取备份文件
            return this.loadBak();
        }
    }

    /**
     * 配置文件路径
     *
     * @return 配置文件路径
     */
    public abstract String configFilePath();

    /**
     * 读取备份文件
     */
    private boolean loadBak() {
        String fileName = null;
        try {
            //配置文件路径
            fileName = this.configFilePath();
            //读取备份文件内容
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                //对文件进行解码
                this.decode(jsonString);
                //打印日志
                log.info("load " + fileName + " OK");
                //返回加载文件成功
                return true;
            }
        } catch (Exception e) {
            //打印日志
            log.error("load " + fileName + " Failed", e);
            //返回加载文件失败
            return false;
        }
        //返回加载文件成功
        return true;
    }

    //对文件内容进行解码
    public abstract void decode(final String jsonString);

    /**
     * 将数据内容持久化到文件中
     */
    public synchronized void persist() {
        //对当前类进行编码
        String jsonString = this.encode(true);
        if (jsonString != null) {
            //获取文件路径
            String fileName = this.configFilePath();
            try {
                //将字符串内容存储到文件中
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                //出现异常打印日志
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    /**
     * 编码
     *
     * @param prettyFormat 是否需要格式化
     * @return 编码后的字符串
     */
    public abstract String encode(final boolean prettyFormat);
}
