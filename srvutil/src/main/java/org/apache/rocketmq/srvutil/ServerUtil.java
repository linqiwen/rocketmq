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

import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
/**
 * 命令行工具
 */
public class ServerUtil {

    /**
     * 生成命令行选项
     *
     * @param options 选项列表
     */
    public static Options buildCommandlineOptions(final Options options) {
        //命令行help选项
        Option opt = new Option("h", "help", false, "Print help");
        //非必填
        opt.setRequired(false);
        //将选项加入到选项列表中
        options.addOption(opt);

        //命令行NameSrv地址选项
        opt =
            new Option("n", "namesrvAddr", true,
                "Name server address list, eg: 192.168.0.1:9876;192.168.0.2:9876");
        //非必填
        opt.setRequired(false);
        //将选项加入到选项列表中
        options.addOption(opt);

        return options;
    }

    /**
     * 解析命令行
     *
     * @param appName 模块名称
     * @param args 参数
     * @param options 命令行选项
     * @param parser 解析器
     */
    public static CommandLine parseCmdLine(final String appName, String[] args, Options options,
        CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            //根据命令行选项从参数中获取选项值
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                //如果选项有h，打印日志
                hf.printHelp(appName, options, true);
                return null;
            }
        } catch (ParseException e) {
            hf.printHelp(appName, options, true);
        }
        //返回命令行
        return commandLine;
    }

    /**
     * 打印命令行帮助
     */
    public static void printCommandLineHelp(final String appName, final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options, true);
    }

    public static Properties commandLine2Properties(final CommandLine commandLine) {
        //创建一个属性列表
        Properties properties = new Properties();
        //获取所有命令行选项
        Option[] opts = commandLine.getOptions();

        if (opts != null) {
            //遍历所有的命令行选项
            for (Option opt : opts) {
                //获取选项名称
                String name = opt.getLongOpt();
                //获取选项值
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    //选项值不为空，设置到属性列表中
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }

}
