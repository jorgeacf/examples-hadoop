package com.jorgefigueiredo.mr;

public interface KeyValue<K, V> {

    K getKey();
    void setKey(K key);

    V getValue();
    void setValue(V value);
}
