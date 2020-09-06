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

package org.apache.rocketmq.common.protocol.body;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
/**
 * 集群信息
 */
public class ClusterInfo extends RemotingSerializable {
    /**
     * broker名称，broker数据信息
     * 主从是相同broker名称，不同brokerId
     */
    private HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    /**
     * 集群名称，集群里的所有broker，主从brokerName相同
     */
    private HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    public HashMap<String, BrokerData> getBrokerAddrTable() {
        return brokerAddrTable;
    }

    public void setBrokerAddrTable(HashMap<String, BrokerData> brokerAddrTable) {
        this.brokerAddrTable = brokerAddrTable;
    }

    public HashMap<String, Set<String>> getClusterAddrTable() {
        return clusterAddrTable;
    }

    public void setClusterAddrTable(HashMap<String, Set<String>> clusterAddrTable) {
        this.clusterAddrTable = clusterAddrTable;
    }

    /**
     * 获取集群里的所有broker地址
     *
     * @param cluster 集群名称
     * @return 集群里的所有broker地址列表
     */
    public String[] retrieveAllAddrByCluster(String cluster) {
        List<String> addrs = new ArrayList<String>();
        if (clusterAddrTable.containsKey(cluster)) {
            //根据集群名称获取所有的broker名称，主从broker名称一样，brokerId不一样
            Set<String> brokerNames = clusterAddrTable.get(cluster);
            //遍历所有broker名称
            for (String brokerName : brokerNames) {
                //获取broker名称所对应的broker数据
                BrokerData brokerData = brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    //将brokerName里的所有broker地址加到列表中
                    addrs.addAll(brokerData.getBrokerAddrs().values());
                }
            }
        }

        return addrs.toArray(new String[] {});
    }

    public String[] retrieveAllClusterNames() {
        return clusterAddrTable.keySet().toArray(new String[] {});
    }
}
