package com.jorgefigueiredo.jobs;

import com.jorgefigueiredo.data.UsersData;
import com.jorgefigueiredo.mr.Context;
import com.jorgefigueiredo.mr.MapReduceProgram;

import java.util.List;

public class MapperSideJoin implements MapReduceProgram<Integer, String, Integer, String> {

    private final List<UsersData.User> users = new UsersData().getAllUsers();

    public void map(Integer key, String value, Context context) {

        String[] tokens = value.split(",");

        UsersData.User user = UsersData.getUserById(users, Integer.parseInt(tokens[1]));

        tokens[1] = user.getUsername();

        String result = String.join(",", tokens);

        context.write(key, result);
    }

    public void reduce(Integer key, Iterable<String> value, Context context) {


        context.write(key, value.iterator().next());
    }

}
