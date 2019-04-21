package com.fuyi.ct.analysis.converter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 用于缓存已知的维度id，减少对mysql的操作次数，提高效率
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = -5907797767584803517L;
    protected int maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return (size() > this.maxElements);
    }
}
