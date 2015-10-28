package com.jorgefigueiredo.mr;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.Iterator;
import java.util.List;

import static com.jorgefigueiredo.mr.TestData.TEXT;

public class MapReduceExecutorTest
    extends TestCase
{

    public MapReduceExecutorTest(String testName)
    {
        super( testName );
    }

    public static Test suite()
    {
        return new TestSuite( MapReduceExecutorTest.class );
    }

    public void testApp()
    {
        assertTrue( true );
    }

    public void testMapReduceExecutor() {

        SingleThreadMapReduceExecutor executor = new SingleThreadMapReduceExecutor();

        List<OutputKeyValue> outputKeyValues = executor.execute(new MapReduceProgram<Integer, String, String, Integer>() {

            private final int ID = 1;

            public void map(Integer key, String value, Context context) {

                context.write(value, ID);
            }

            public void reduce(String key, Iterable<Integer> values, Context context) {

                int count = 0;
                Iterator<Integer> itr = values.iterator();
                while (itr.hasNext()) {
                    itr.next();
                    count++;
                }

                context.write(key, count);
            }

        }, TEXT);

        assertEquals(3, outputKeyValues.size());
        assertEquals("for", outputKeyValues.get(0).getKey());
        assertEquals("the", outputKeyValues.get(1).getKey());
        assertEquals("when", outputKeyValues.get(2).getKey());

        assertEquals(3, outputKeyValues.get(0).getValue());
        assertEquals(2, outputKeyValues.get(1).getValue());
        assertEquals(2, outputKeyValues.get(2).getValue());

        System.out.print(executor.getStatistics().toString());
    }

}
