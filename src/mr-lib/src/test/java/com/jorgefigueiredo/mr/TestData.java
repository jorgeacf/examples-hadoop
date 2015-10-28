package com.jorgefigueiredo.mr;


import com.google.common.collect.ImmutableList;

import java.util.List;

public final class TestData {

    public static List<InputRecord> TEXT =
            new ImmutableList.Builder<InputRecord>()
            .add(new InputRecord<Integer, String>(1, "the"))
            .add(new InputRecord<Integer, String>(1, "for"))
            .add(new InputRecord<Integer, String>(1, "the"))
            .add(new InputRecord<Integer, String>(1, "for"))
            .add(new InputRecord<Integer, String>(1, "for"))
            .add(new InputRecord<Integer, String>(1, "when"))
            .add(new InputRecord<Integer, String>(1, "when"))
            .build().asList();


}
