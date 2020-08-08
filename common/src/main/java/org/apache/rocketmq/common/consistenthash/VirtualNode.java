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
package org.apache.rocketmq.common.consistenthash;

/**
 * 虚拟节点，将物理节点散列成虚拟节点加入到哈希环中
 */
public class VirtualNode<T extends Node> implements Node {
    /**
     * 物理节点
     */
    final T physicalNode;
    /**
     * 下标，物理节点的第几个虚拟节点
     * 从0开始，replicaIndex等于0，表示物理节点的一个虚拟节点，以此类推
     */
    final int replicaIndex;

    public VirtualNode(T physicalNode, int replicaIndex) {
        this.replicaIndex = replicaIndex;
        this.physicalNode = physicalNode;
    }

    /**
     * 虚拟节点key
     */
    @Override
    public String getKey() {
        return physicalNode.getKey() + "-" + replicaIndex;
    }

    /**
     * 判断是否是pNode的虚拟节点
     */
    public boolean isVirtualNodeOf(T pNode) {
        return physicalNode.getKey().equals(pNode.getKey());
    }

    /**
     * 获取当前虚拟节点的物理节点
     */
    public T getPhysicalNode() {
        return physicalNode;
    }
}
