package com.feng.easykv.core.store;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description: 内存存储
 * @Author: txf
 * @Date: 2026/2/10
 */
public class MemoryStorage {
    private ConcurrentHashMap<String, Object> kvMap;

    public MemoryStorage() {
        kvMap = new ConcurrentHashMap<>();
    }

    public Object get(String key) {
        return kvMap.get(key);
    }

    public void put(String key, Object value) {
        kvMap.put(key, value);
    }

    public void delete(String key) {
        kvMap.remove(key);
    }
}
