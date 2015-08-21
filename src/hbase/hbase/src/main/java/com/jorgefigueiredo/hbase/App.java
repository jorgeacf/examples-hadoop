package com.jorgefigueiredo.hbase;

import java.io.IOException;

import com.jorgefigueiredo.hbase.entity.User;
import com.jorgefigueiredo.hbase.repository.IUserRepository;
import com.jorgefigueiredo.hbase.repository.UserRepository;


public class App 
{
    public static void main( String[] args ) throws IOException
    {
        IUserRepository repository = new UserRepository();
        
        for(User user : repository.readAll()) {
        	System.out.println(user);
        }
        
        
    }
}
