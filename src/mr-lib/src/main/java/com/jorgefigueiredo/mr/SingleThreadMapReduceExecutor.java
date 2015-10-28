package com.jorgefigueiredo.mr;

import java.util.*;


public final class SingleThreadMapReduceExecutor {

    private final MapReduceProgramStatistics statistics = new MapReduceProgramStatistics();

    public List<OutputKeyValue> execute(MapReduceProgram program, List<InputRecord> inputValues) {

        Context mapperContext = new Context(KeyValuePair.class);

        //
        //  Mapper
        //
        statistics.setInputRecordsReaded(inputValues.size());
        for(InputRecord input : inputValues) {

            program.map(input.getKey(), input.getValue(), mapperContext);
        }

        List mapperValues = mapperContext.getValues();
        statistics.setMapperKeyValuePairsEmitted(mapperValues.size());

        //
        //  Shuffle and sort
        //
        Map map = shuffleKeyValuePairs(mapperValues);

        Context reducerContext = new Context(OutputKeyValue.class);


        //
        //  Reduce
        //
        statistics.setReducerKeysProcessed(map.keySet().size());
        for(Object mapperValue : map.keySet()) {

            program.reduce(mapperValue, (Iterable)map.get(mapperValue), reducerContext);
        }

        List outputValues = reducerContext.getValues();

        statistics.setReducerKeyValuesEmitted(outputValues.size());

        return outputValues;
    }

    public MapReduceProgramStatistics getStatistics() {
        return statistics;
    }

    private Map shuffleKeyValuePairs(List<KeyValue> mappersOutput) {

        Map keyValueMap = new TreeMap();

        for(KeyValue keyvalue : mappersOutput) {

            if(keyValueMap.containsKey(keyvalue.getKey())) {
                List keyValues = (List)keyValueMap.get(keyvalue.getKey());
                keyValues.add(keyvalue.getValue());
                keyValueMap.put(keyvalue.getKey(), keyValues);
            }
            else {
                List keyValues = new LinkedList();
                keyValues.add(keyvalue.getValue());
                keyValueMap.put(keyvalue.getKey(), keyValues);
            }
        }

        return keyValueMap;
    }

}
