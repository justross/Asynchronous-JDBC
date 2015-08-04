package async;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;


public class DatabaseConnection{

	@Rule
	public RepeatRule repeatRule = new RepeatRule();



	//JDBC driver name and database URL
	static final String jdbcDriver = "com.mysql.jdbc.Driver";
	static final String databaseURL = "jdbc:mysql://localhost:3306/async";

	//Database credentials
	static final String user = "root";
	static final String pass = "mysql";

	int threadcount = 3;

	volatile static int count = 0;

	final static int TEST_DURATION = 1800;




	@Test
	@Repeat(times = 20)
	public void singleThreadTest(){
		try{
			count = 0;
			final Random random = new Random();
			long start=System.currentTimeMillis();
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = null;
			connection = DriverManager.getConnection(databaseURL, user, pass);
			String sql = "SELECT SQL_NO_CACHE * from async.test where id=?";
			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet rs = null;
			while(System.currentTimeMillis()-start<TEST_DURATION){
				ps.setInt(1, random.nextInt(30000));
				rs = ps.executeQuery();
				if(rs.next()){
					int id = rs.getInt(1);
					count++;
				}
				rs.next();
				ps.clearParameters();
			}			
			rs.close();
			connection.close();
			System.out.println("Single Thread, Synchronous Connection: "+count);
		}catch (Exception e){
			e.printStackTrace();
		}
	}



	static class MultiThread implements Runnable{
		public void run(){
			try{
				final Random random = new Random();
				long start=System.currentTimeMillis();
				Class.forName("com.mysql.jdbc.Driver");
				Connection connection = null;
				connection = DriverManager.getConnection(databaseURL, user, pass);
				String sql = "SELECT SQL_NO_CACHE * from async.test where id=?";
				PreparedStatement ps = connection.prepareStatement(sql);
				ResultSet rs = null;
				while(System.currentTimeMillis()-start<TEST_DURATION){
					ps.setInt(1, random.nextInt(30000));
					rs = ps.executeQuery();
					if(rs.next()){
						int id = rs.getInt(1);
						increment();
					}
					rs.next();
					ps.clearParameters();
				}			
				rs.close();
				connection.close();
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		public synchronized void increment(){
			count++;
		}
	}




	@Test
	@Repeat(times = 20)
	public void multiThreadTest(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			count = 0;
			Thread[] threadlist = new Thread[threadcount];
			for(int i=0; i < threadcount; i++){
				threadlist[i] = new Thread(new MultiThread());
				threadlist[i].start();
			}
			Thread.sleep(TEST_DURATION);




			System.out.println("Multi Thread, Synchronous Connection: "+count);
		}catch (Exception e){
			e.printStackTrace();
		}
	}


}//end DatabaseConnection
