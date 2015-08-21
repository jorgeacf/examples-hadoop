package com.jorgefigueiredo.hbase.repository;

import java.io.IOException;
import java.util.UUID;

import com.jorgefigueiredo.hbase.entity.User;

public interface IUserRepository {

	User read(UUID userid);
	void insert(User user);
	void update(User user);
	void delete(UUID userid);
	
	Iterable<User> readAll() throws IOException;
	
}
