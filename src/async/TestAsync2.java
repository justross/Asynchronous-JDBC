package async;

import java.sql.SQLException;

import org.async.jdbc.AsyncConnection;
import org.async.jdbc.Connection;
import org.async.jdbc.PreparedQuery;
import org.async.jdbc.PreparedStatement;
import org.async.jdbc.ResultSet;
import org.async.jdbc.ResultSetCallback;
import org.async.jdbc.SuccessCallback;
import org.async.mysql.MysqlConnection;
import org.async.mysql.protocol.packets.OK;
import org.async.net.Multiplexer;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestAsync2 {
	
	@Rule
	public RepeatRule repeatRule = new RepeatRule();

	private final int TEST_DURATION = 60000;

	volatile int count = 0;

	int singleThreadConnections = 3;
	int multiThreadConnections = 3;
	int threadcount = 3;




	@Test
	@Repeat(times = 20)
	public void singleThreadTest() throws IOException {

		count = 0;
		final Random random = new Random();
		Multiplexer mpx = new Multiplexer();
		final List<MysqlConnection> cons = new ArrayList<MysqlConnection>();
		final AtomicBoolean stop=new AtomicBoolean(false);
		for (int i = 0; i < singleThreadConnections; i++) {
			final int idx=i;
			cons.add(new MysqlConnection("localhost", 3306, "root", "mysql", "async", mpx.getSelector(),new SuccessCallback() {

				@Override
				public void onError(SQLException e) {
					e.printStackTrace();

				}

				@Override
				public void onSuccess(OK ok) {
					try {
						final PreparedStatement ps = cons.get(idx).prepareStatement("select SQL_NO_CACHE * from async.test where id=?");

						final PreparedQuery query = new PreparedQuery() {

							@Override
							public void query(PreparedStatement pstmt)
									throws SQLException {
								pstmt.setInteger(1,
										random.nextInt(30000));

							}
						};
						final ResultSetCallback rsc = new ResultSetCallback() {


							@Override
							public void onError(SQLException e) {
								e.printStackTrace();

							}

							@Override
							public void onResultSet(ResultSet rs) {
								while (rs.hasNext()) {
									rs.next();
									int id = rs.getInteger(1);
									//java.sql.Timestamp date = rs.getTimestamp(2);
									//int status = rs.getInteger(3);
									//String text = rs.getString(4);

								}

								try {
									if(!stop.get()) {
										ps.executeQuery(query, this);
									} else {
										cons.get(idx).close();
									}
								} catch (SQLException e) {

									e.printStackTrace();
								}
								count++;

							}
						};
						ps.executeQuery(query, rsc);
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}
			}));
		}

		long start=System.currentTimeMillis();
		while (System.currentTimeMillis()-start<TEST_DURATION) {
			mpx.select();
		}



		stop.set(true);
		for (int i=0;i<10;i++) {
			mpx.select(100);

		}
		System.out.println("Single Thread, "+singleThreadConnections+ " connections: "+count+" reads");
	}


	public synchronized void increment(){
		count++;
	}

@Test
@Repeat(times = 20)
public void multiThreadTest() throws IOException, InterruptedException {
	final ThreadSafeWrapper wrapper = new ThreadSafeWrapper(threadcount);

	count = 0;
	final Random random = new Random();



	for(int i=0;i<multiThreadConnections;i++) {
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					wrapper.q(new ThreadSafeWrapper.Query() {
						PreparedStatement ps;
						final PreparedQuery query = new PreparedQuery() {

							@Override
							public void query(PreparedStatement pstmt)
									throws SQLException {
								pstmt.setInteger(1, random.nextInt(30000));
							}
						};
						final ResultSetCallback rsc = new ResultSetCallback() {

							@Override
							public void onError(SQLException e) {
								e.printStackTrace();

							}

							@Override
							public void onResultSet(ResultSet rs) {
								while (rs.hasNext()) {
									rs.next();
									int id = rs.getInteger(1);
								}

								try {
									ps.executeQuery(query, this);
								} catch (SQLException e) {

									e.printStackTrace();
								}
								increment();

							}
						};
						@Override
						public void doInConnection(MysqlConnection con) {
							try {
								if(ps==null) ps=con.prepareStatement("select SQL_NO_CACHE * from async.test where id=?");
								ps.executeQuery(query, rsc);
							} catch (SQLException e) {
								e.printStackTrace();
							}


						}

					});
				} catch (InterruptedException e) {
					return;

				}

			}

		}).start();


	}

	Thread.sleep(TEST_DURATION);


	wrapper.stop();
	System.out.println(threadcount+" Threads, "+multiThreadConnections+" connections: "+count+" reads");

}




}