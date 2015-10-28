package com.jorgefigueiredo.mr.com.jorgefigueiredo.jobs;

import com.jorgefigueiredo.data.PostsData;
import com.jorgefigueiredo.jobs.ReduceSideOneToOneJoin;
import com.jorgefigueiredo.mr.OutputKeyValue;
import com.jorgefigueiredo.mr.SingleThreadMapReduceExecutor;
import junit.framework.TestCase;

import java.util.List;

public class ReduceSideOneToOneJoinTest extends TestCase {

    public void testJoin() {

        SingleThreadMapReduceExecutor  executor = new SingleThreadMapReduceExecutor();

        List<OutputKeyValue> outputKeyValues = executor.execute(new ReduceSideOneToOneJoin(), new PostsData().getAllPosts());

        assertEquals(5, outputKeyValues.size());
        assertEquals("user1", outputKeyValues.get(0).getKey());
        assertEquals("user2", outputKeyValues.get(1).getKey());
        assertEquals("user3", outputKeyValues.get(2).getKey());
        assertEquals("user4", outputKeyValues.get(3).getKey());
        assertEquals("user6", outputKeyValues.get(4).getKey());

        assertEquals("this is the post4 content.", outputKeyValues.get(0).getValue());
        assertEquals("this is the post2 content.", outputKeyValues.get(1).getValue());
        assertEquals("this is the post1 content.,this is the post6 content.", outputKeyValues.get(2).getValue());
        assertEquals("this is the post3 content.", outputKeyValues.get(3).getValue());
        assertEquals("this is the post5 content.", outputKeyValues.get(4).getValue());


    }

}
