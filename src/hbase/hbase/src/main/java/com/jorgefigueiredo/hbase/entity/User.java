package com.jorgefigueiredo.hbase.entity;

import java.util.UUID;

public class User {

	private UUID userId;
	private String username;
	
	private String firstname;
	private String lastname;
	
	private String email;
	
	public User() {
		
	}
	
	public User(UUID userId, String username, String firstname, String lastname, String email) {
		this.userId = userId;
		this.username = username;
		this.firstname = firstname;
		this.lastname = lastname;
		this.email = email;
	}

	public UUID getUserId() {
		return userId;
	}

	public void setUserId(UUID userId) {
		this.userId = userId;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
	
	@Override
	public String toString() {
		return String.format("User(Userid, Username, Firstname, Lastname, Email) [%s, %s, %s, %s, %s]", 
				userId.toString(), username, firstname, lastname, email);
	}
}
