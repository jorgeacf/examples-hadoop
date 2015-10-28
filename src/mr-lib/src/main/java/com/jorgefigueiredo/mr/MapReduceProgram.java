package com.jorgefigueiredo.mr;


public interface MapReduceProgram<K1, V1, K2, V2> {

    void map(K1 key, V1 value, Context context);
    void reduce(K2 key, Iterable<V2> values, Context context);

}
