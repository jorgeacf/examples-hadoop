package com.jorgefigueiredo.data;

import com.google.common.collect.ImmutableList;
import com.jorgefigueiredo.mr.InputRecord;

import java.util.List;

public final class PostsData {

    public List<InputRecord> getAllPosts() {
        return new ImmutableList.Builder<InputRecord>()
                .add(new InputRecord(1, "1,3,this is the post1 content.,28102015"))
                .add(new InputRecord(2, "2,2,this is the post2 content.,28102015"))
                .add(new InputRecord(3, "3,4,this is the post3 content.,28102015"))
                .add(new InputRecord(4, "4,1,this is the post4 content.,28102015"))
                .add(new InputRecord(5, "5,6,this is the post5 content.,28102015"))
                .add(new InputRecord(6, "6,3,this is the post6 content.,28102015"))
                .build().asList();
    }

}
