package com.jorgefigueiredo.mr;

import java.util.LinkedList;
import java.util.List;

public class Context<T extends KeyValue> {

    private final Class<?> contextType;
    private List<T> keyValuePairs = new LinkedList<T>();

    public Context(Class<?> contextType) {
        this.contextType = contextType;
    }

    public void write(Object key, Object value) {

        T t = makeInstance(contextType);
        t.setKey(key);
        t.setValue(value);

        keyValuePairs.add(t);
    }

    public List<T> getValues() {
        return keyValuePairs;
    }

    private T makeInstance(Class<?> instanceType) {

        try {
            return (T)instanceType.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        throw new RuntimeException();
    }

}
