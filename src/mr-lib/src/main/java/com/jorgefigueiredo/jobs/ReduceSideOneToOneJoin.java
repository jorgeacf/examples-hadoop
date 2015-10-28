package com.jorgefigueiredo.jobs;

import com.jorgefigueiredo.data.UsersData;
import com.jorgefigueiredo.mr.Context;
import com.jorgefigueiredo.mr.MapReduceProgram;

import java.util.List;

public class ReduceSideOneToOneJoin implements MapReduceProgram<Integer, String, Integer, String> {

    private final List<UsersData.User> users = new UsersData().getAllUsers();

    public void map(Integer key, String value, Context context) {

        String[] tokens = value.split(",");

        context.write(Integer.parseInt(tokens[1]), tokens[2]);
    }

    public void reduce(Integer key, Iterable<String> values, Context context) {

        UsersData.User user = UsersData.getUserById(users, key);

        String result = String.join(",", values);

        context.write(user.getUsername(), result);
    }
}
