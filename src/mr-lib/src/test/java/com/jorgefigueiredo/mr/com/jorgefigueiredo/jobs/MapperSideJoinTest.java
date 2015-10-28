package com.jorgefigueiredo.mr.com.jorgefigueiredo.jobs;

import com.jorgefigueiredo.data.PostsData;
import com.jorgefigueiredo.jobs.MapperSideJoin;
import com.jorgefigueiredo.mr.OutputKeyValue;
import com.jorgefigueiredo.mr.SingleThreadMapReduceExecutor;
import junit.framework.TestCase;

import java.util.List;

public class MapperSideJoinTest extends TestCase {

    public void testMapperSideJoin() {

        SingleThreadMapReduceExecutor executor = new SingleThreadMapReduceExecutor();

        List<OutputKeyValue> outputKeyValues = executor.execute(new MapperSideJoin(), new PostsData().getAllPosts());

        assertEquals(6, outputKeyValues.size());

        assertEquals("1,user3,this is the post1 content.,28102015", outputKeyValues.get(0).getValue());
        assertEquals("2,user2,this is the post2 content.,28102015", outputKeyValues.get(1).getValue());
        assertEquals("3,user4,this is the post3 content.,28102015", outputKeyValues.get(2).getValue());
        assertEquals("4,user1,this is the post4 content.,28102015", outputKeyValues.get(3).getValue());
        assertEquals("5,user6,this is the post5 content.,28102015", outputKeyValues.get(4).getValue());
        assertEquals("6,user3,this is the post6 content.,28102015", outputKeyValues.get(5).getValue());
    }

}
