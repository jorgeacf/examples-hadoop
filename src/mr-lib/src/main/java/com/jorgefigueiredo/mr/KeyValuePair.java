package com.jorgefigueiredo.mr;

public class KeyValuePair<K,V> implements KeyValue<K,V> {

    private K key;
    private V value;

    public KeyValuePair() { }

    public KeyValuePair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }
    public void setKey(K key) { this.key = key; }

    public V getValue() {
        return value;
    }
    public void setValue(V value) { this.value = value; }
}
