package com.jorgefigueiredo.mr;

public class InputRecord<K, V> {
    private final K key;
    private final V value;

    public InputRecord(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}
