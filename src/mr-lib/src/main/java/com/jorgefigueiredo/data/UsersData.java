package com.jorgefigueiredo.data;

import com.google.common.collect.ImmutableList;
import com.jorgefigueiredo.mr.InputRecord;

import java.util.LinkedList;
import java.util.List;

public class UsersData {

    public List<InputRecord> getInputRecords() {
        return new ImmutableList.Builder<InputRecord>()
                .add(new InputRecord(1, "1,user1,user1@mail.com"))
                .add(new InputRecord(2, "2,user2,user2@mail.com"))
                .add(new InputRecord(3, "3,user3,user3@mail.com"))
                .add(new InputRecord(4, "4,user4,user4@mail.com"))
                .add(new InputRecord(5, "5,user5,user5@mail.com"))
                .add(new InputRecord(6, "6,user6,user6@mail.com"))
                .build().asList();
    }

    public List<User> getAllUsers() {

        List<User> users = new LinkedList<User>();

        for(InputRecord record : getInputRecords()) {

            String[] tokens = record.getValue().toString().split(",");
            users.add(new User(Integer.parseInt(tokens[0]), tokens[1], tokens[2]));
        }

        return new ImmutableList.Builder<User>()
                .addAll(users)
                .build().asList();
    }

    public static UsersData.User getUserById(List<UsersData.User> users, int id) {

        for(UsersData.User user : users) {
            if(user.getId() == id) {
                return user;
            }
        }
        return null;
    }

    public static class User {

        private final int id;
        private final String username;
        private final String email;

        public User(int id, String username, String email) {
            this.id = id;
            this.username = username;
            this.email = email;
        }

        public int getId() {
            return id;
        }

        public String getUsername() {
            return username;
        }

        public String getEmail() {
            return email;
        }
    }


}
