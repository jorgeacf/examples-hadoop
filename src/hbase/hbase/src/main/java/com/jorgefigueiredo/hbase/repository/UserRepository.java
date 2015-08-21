package com.jorgefigueiredo.hbase.repository;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.jorgefigueiredo.hbase.entity.User;

public class UserRepository extends AbstractHBaseRepository implements IUserRepository {

	private static final String TABLE_NAME = "users";

	private static final byte[] CF_INFO = Bytes.toBytes("cf_info");
	
	private static final byte[] qUserid = Bytes.toBytes("userid");
	private static final byte[] qUsername = Bytes.toBytes("username");
	private static final byte[] qFirstname = Bytes.toBytes("firstname");
	private static final byte[] qLastname = Bytes.toBytes("lastname");
	private static final byte[] qEmail = Bytes.toBytes("email");
	
	public UserRepository() throws IOException {
		super(TABLE_NAME);
	}
	
	public User read(UUID userid) {
		
		byte[] key = Bytes.toBytes(userid.toString());
		
		Scan scan = new Scan(key);
		
		PrefixFilter filter = new PrefixFilter(key);
		
		scan.setFilter(filter);
		
		try {
			
			ResultScanner resultScanner = htable.getScanner(scan);
			
			Result result = resultScanner.next();
			
			if(result != null) {
				return convert(result);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	public void insert(User user) {
		
		Put p = new Put(Bytes.toBytes(user.getUserId().toString()));
		
		p.add(CF_INFO, qUserid, Bytes.toBytes(user.getUserId().toString()));
		p.add(CF_INFO, qUsername, Bytes.toBytes(user.getUsername()));
		p.add(CF_INFO, qFirstname, Bytes.toBytes(user.getFirstname()));
		p.add(CF_INFO, qLastname, Bytes.toBytes(user.getLastname()));
		p.add(CF_INFO, qEmail, Bytes.toBytes(user.getEmail()));
		
		try {
			
			htable.put(p);
			
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
	}

	public void update(User user) {
		
	}

	public void delete(UUID userid) {
		
		byte[] key = Bytes.toBytes(userid.toString());
		
		Delete d = new Delete(key);
		
		try {
			
			htable.delete(d);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public Iterable<User> readAll() throws IOException {
		
		Scan s = new Scan();
		
		final ResultScanner scanner = htable.getScanner(s);
		
		return new Iterable<User>() {

			public Iterator<User> iterator() {

				return new Iterator<User>() {

					Result result = null;
					public boolean hasNext() {
						
						try {
							
							result = scanner.next();
							
						} catch (IOException e) {
							e.printStackTrace();
						}
						
						return result != null;
					}

					public User next() {
						
						if(result == null) {
							try {
								
								scanner.next();
								
							} catch (IOException e) {
								return null;
							}
						}
						
						if(result == null) {
							return null;
						}
						
						User user = convert(result);
						
						result = null;
						
						return user;
						
					}
				};
			} };
		

		}

	private User convert(Result result) {
		User user = new User(
				UUID.fromString(Bytes.toString(result.getValue(CF_INFO, qUserid))),
				Bytes.toString(result.getValue(CF_INFO, qUsername)),
				Bytes.toString(result.getValue(CF_INFO, qFirstname)),
				Bytes.toString(result.getValue(CF_INFO, qLastname)),
				Bytes.toString(result.getValue(CF_INFO, qEmail))
				);
		return user;
	}
}
