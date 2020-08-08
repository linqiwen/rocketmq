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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * To hash Node objects to a hash ring with a certain amount of virtual node.
 * Method routeNode will return a Node instance which the object key should be allocated to according to consistent hash
 * algorithm
 */
public class ConsistentHashRouter<T extends Node> {
    private final SortedMap<Long, VirtualNode<T>> ring = new TreeMap<Long, VirtualNode<T>>();
    /**
     * hash函数
     */
    private final HashFunction hashFunction;

    public ConsistentHashRouter(Collection<T> pNodes, int vNodeCount) {
        this(pNodes, vNodeCount, new MD5Hash());
    }

    /**
     * @param pNodes 物理节点的集合
     * @param vNodeCount 虚拟节点数量
     * @param hashFunction hash Function to hash Node instances
     */
    public ConsistentHashRouter(Collection<T> pNodes, int vNodeCount, HashFunction hashFunction) {
        if (hashFunction == null) {
            throw new NullPointerException("Hash Function is null");
        }
        this.hashFunction = hashFunction;
        if (pNodes != null) {
            for (T pNode : pNodes) {
                addNode(pNode, vNodeCount);
            }
        }
    }

    /**
     * 物理节点以一些虚拟节点添加到哈希环
     *
     * @param pNode 需要添加到哈希环的物理节点
     * @param vNodeCount 物理节点的虚拟节点的数量。值应大于或等于0
     */
    public void addNode(T pNode, int vNodeCount) {
        if (vNodeCount < 0)
            throw new IllegalArgumentException("illegal virtual node counts :" + vNodeCount);
        //获取散列环中当前物理节点的虚拟节点数
        int existingReplicas = getExistingReplicas(pNode);
        //将物理节点散列成vNodeCount个虚拟节点加入到散列环中
        for (int i = 0; i < vNodeCount; i++) {
            //创建虚拟节点
            VirtualNode<T> vNode = new VirtualNode<T>(pNode, i + existingReplicas);
            ring.put(hashFunction.hash(vNode.getKey()), vNode);
        }
    }

    /**
     * 移除物理节点的哈希环
     * <p>
     *     将当前物理节点的所有虚拟节点从哈希环中移除
     * </p>
     */
    public void removeNode(T pNode) {
        Iterator<Long> it = ring.keySet().iterator();
        while (it.hasNext()) {
            Long key = it.next();
            VirtualNode<T> virtualNode = ring.get(key);
            if (virtualNode.isVirtualNodeOf(pNode)) {
                it.remove();
            }
        }
    }

    /**
     * 使用指定的键，路由当前哈希环中最近的节点实例
     *
     * @param objectKey 使用键查找最近节点实例
     * @return 最近节点实例所对应的物理节点
     */
    public T routeNode(String objectKey) {
        if (ring.isEmpty()) {
            return null;
        }
        //将传入进来的key哈希成哈希值
        Long hashVal = hashFunction.hash(objectKey);
        //返回大于或等于此映射值的视图
        SortedMap<Long, VirtualNode<T>> tailMap = ring.tailMap(hashVal);
        //如果大于或等于此映射值的视图不为空，取视图的第一个元素，否则取哈希环的一个元素，因为没有比映射值更大或者相等的元素
        //顺时针取哈希环的第一个元素
        Long nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        //返回虚拟节点的物理节点
        return ring.get(nodeHashVal).getPhysicalNode();
    }

    /**
     * 获取物理节点pNode在哈希环中的虚拟节点数
     */
    public int getExistingReplicas(T pNode) {
        int replicas = 0;
        for (VirtualNode<T> vNode : ring.values()) {
            if (vNode.isVirtualNodeOf(pNode)) {
                replicas++;
            }
        }
        return replicas;
    }

    //默认的hash函数
    private static class MD5Hash implements HashFunction {
        MessageDigest instance;

        public MD5Hash() {
            try {
                instance = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
            }
        }

        @Override
        public long hash(String key) {
            instance.reset();
            instance.update(key.getBytes());
            byte[] digest = instance.digest();

            long h = 0;
            for (int i = 0; i < 4; i++) {
                h <<= 8;
                h |= ((int) digest[i]) & 0xFF;
            }
            return h;
        }
    }

}
