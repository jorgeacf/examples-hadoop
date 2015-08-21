package com.jorgefigueiredo.hbase.repository.tests;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import com.jorgefigueiredo.hbase.entity.User;
import com.jorgefigueiredo.hbase.repository.IUserRepository;
import com.jorgefigueiredo.hbase.repository.UserRepository;

public class UserRepositoryTests {

	@Test
	public void testCreateUser() throws IOException {
		
		IUserRepository userRepository = new UserRepository();
		
		UUID userId = UUID.randomUUID();
		
		userRepository.insert(
				new User(
						userId,
						"a",
						"b",
						"c",
						"d"));
		
		User user = userRepository.read(userId);
		
		userRepository.delete(userId);
		
		assertEquals(userId, user.getUserId());
		assertEquals("a", user.getUsername());
		assertEquals("b", user.getFirstname());
		assertEquals("c", user.getLastname());
		assertEquals("d", user.getEmail());
		
	}
	
	@Test
	public void testReadUser() throws IOException {
		
		IUserRepository userRepository = new UserRepository();
		
		UUID userId = UUID.randomUUID();
		
		userRepository.insert(
				new User(
						userId,
						"a",
						"b",
						"c",
						"d"));
		
		User user = userRepository.read(userId);

		userRepository.delete(userId);
		
		assertEquals(userId, user.getUserId());
		assertEquals("a", user.getUsername());
		assertEquals("b", user.getFirstname());
		assertEquals("c", user.getLastname());
		assertEquals("d", user.getEmail());
	}
	
	@Test
	public void testDeleteUser() throws IOException {
		
		IUserRepository userRepository = new UserRepository();
		
		UUID userId = UUID.randomUUID();
		
		userRepository.insert(
				new User(
						userId,
						"a",
						"b",
						"c",
						"d"));
		
		User b_user = userRepository.read(userId);
		
		userRepository.delete(userId);
		
		User a_user = userRepository.read(userId);
		
		assertEquals(userId, b_user.getUserId());
		assertEquals("a", b_user.getUsername());
		assertEquals("b", b_user.getFirstname());
		assertEquals("c", b_user.getLastname());
		assertEquals("d", b_user.getEmail());
		
		assertNull(a_user);
	}

}
