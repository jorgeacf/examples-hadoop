package com.jorgefigueiredo.mr;

public class MapReduceProgramStatistics {

    private int inputRecordsRead;
    private int mapperKeyValuePairsEmitted;

    private int reducerKeysProcessed;
    private int reducerKeyValuesEmitted;



    public void incrInputRecordsReaded() {
        inputRecordsRead++;
    }

    public void incrMapperKeyValuePairsEmitted() { mapperKeyValuePairsEmitted++; }

    public void incrReducerKeysProcessed() {
        reducerKeysProcessed++;
    }

    public void incrReducerKeyValuesEmitted() {
        reducerKeyValuesEmitted++;
    }

    public void setInputRecordsReaded(int value) {
        inputRecordsRead = value;
    }

    public void setMapperKeyValuePairsEmitted(int value) { mapperKeyValuePairsEmitted = value; }

    public void setReducerKeysProcessed(int value) {
        reducerKeysProcessed = value;
    }

    public void setReducerKeyValuesEmitted(int value) {
        reducerKeyValuesEmitted = value;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Input Records Read = " + inputRecordsRead + "\n");
        sb.append("Mapper KeyValue Pairs Emitted = " + mapperKeyValuePairsEmitted + "\n");

        sb.append("Reducer Keys Processed = " + reducerKeysProcessed + "\n");
        sb.append("Reducer KeyValues Emitted = " + reducerKeyValuesEmitted + "\n");

        return sb.toString();
    }

}
