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

package org.apache.rocketmq.srvutil;

import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 文件监视服务
 * <p>
 *     如果文件改变，回调监听器进行处理
 * </p>
 */
public class FileWatchService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 被监视的文件路径列表
     */
    private final List<String> watchFiles;
    /**
     * 被监视文件内容hash列表
     */
    private final List<String> fileCurrentHash;
    /**
     * 监听器
     */
    private final Listener listener;
    /**
     * 监听频率
     */
    private static final int WATCH_INTERVAL = 500;
    /**
     * 消息摘要算法
     */
    private MessageDigest md = MessageDigest.getInstance("MD5");

    public FileWatchService(final String[] watchFiles,
        final Listener listener) throws Exception {
        this.listener = listener;
        this.watchFiles = new ArrayList<>();
        this.fileCurrentHash = new ArrayList<>();

        //遍历传入进来的文件路径列表
        for (int i = 0; i < watchFiles.length; i++) {
            //文件路径不为空，并且文件存在
            if (!Strings.isNullOrEmpty(watchFiles[i]) && new File(watchFiles[i]).exists()) {
                //将文件路径加入到被监视的文件路径列表中
                this.watchFiles.add(watchFiles[i]);
                //将文件内容hash值加入到被监视文件内容hash列表
                this.fileCurrentHash.add(hash(watchFiles[i]));
            }
        }
    }

    /**
     * 获取服务名
     */
    @Override
    public String getServiceName() {
        return "FileWatchService";
    }

    @Override
    public void run() {
        //打印日志
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                //等待500毫秒后执行
                this.waitForRunning(WATCH_INTERVAL);
                //遍历文件路径
                for (int i = 0; i < watchFiles.size(); i++) {
                    String newHash;
                    try {
                        //获取文件内容hash值
                        newHash = hash(watchFiles.get(i));
                    } catch (Exception ignored) {
                        //出现异常打印日志
                        log.warn(this.getServiceName() + " service has exception when calculate the file hash. ", ignored);
                        continue;
                    }
                    //如果hash值不相等表明文件已经被修改
                    if (!newHash.equals(fileCurrentHash.get(i))) {
                        //重新设置hash值
                        fileCurrentHash.set(i, newHash);
                        //执行监听器的回调方法
                        listener.onChanged(watchFiles.get(i));
                    }
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    private String hash(String filePath) throws IOException, NoSuchAlgorithmException {
        //获取文件路径
        Path path = Paths.get(filePath);
        //读取文件 内容到MessageDigest中
        md.update(Files.readAllBytes(path));
        //获取内容的hash字节数组
        byte[] hash = md.digest();
        //将字节数组转成字符串
        return UtilAll.bytes2string(hash);
    }

    public interface Listener {
        /**
         * Will be called when the target files are changed
         * @param path the changed file path
         */
        void onChanged(String path);
    }
}
