package com.huyida.consistenthash.infrastructure;

import com.huyida.consistenthash.infrastructure.base.Hashing;
import lombok.Data;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @program: load-balancer
 * @description:
 * @author: huyida
 * @create: 2022-06-18 12:17
 **/
@Data
public class ConsistentHash<T> {

    private final Hashing hashing;

    private final int numberOfReplicas;

    private final SortedMap<Long, T> circle = new TreeMap<>();

    public ConsistentHash(Hashing hashing, int numberOfReplicas, Collection<T> nodes) {
        super();
        this.hashing = hashing;
        this.numberOfReplicas = numberOfReplicas;
        for (T node : nodes) {
            add(node);
        }
    }

    /**
     * 增加节点
     *
     * @param node
     */
    public void add(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.put(hashing.hash(node.toString() + i), node);
        }
    }

    /**
     * 删除节点
     *
     * @param node
     */
    public void remove(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hashing.hash(node.toString() + i));
        }
    }

    /**
     * 获取节点
     *
     * @param key
     * @return
     */
    public T get(String key) {
        if (circle.isEmpty()) {
            return null;
        }
        long hash = this.hashing.hash(key);
        SortedMap<Long, T> tailMap = circle.tailMap(hash);
        if (tailMap.isEmpty()) {
            return circle.get(circle.firstKey());
        }
        return tailMap.get(tailMap.firstKey());
    }

}
