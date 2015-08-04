package async;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;




import org.async.jdbc.SuccessCallback;
import org.async.mysql.MysqlConnection;
import org.async.mysql.protocol.packets.OK;
import org.async.net.Multiplexer;

public class ThreadSafeWrapper {
	public interface Query {
		void doInConnection(MysqlConnection con);

	}
	final ArrayBlockingQueue<Query> q=new ArrayBlockingQueue<Query>(64);
	ExecutorService pool;
	List<Multiplexer> mpxs=new ArrayList<Multiplexer>();
	public  ThreadSafeWrapper(int mpx) throws IOException {
		pool = Executors.newFixedThreadPool(mpx);

		for(int i=0;i<mpx;i++) {
			final Multiplexer multiplexer = new Multiplexer();
			mpxs.add(multiplexer);
			pool.execute(new Runnable() {
				@Override
				public void run() {
					while(true) {
						try {
							multiplexer.select();
							final Query e = q.poll();
							if(e!=null) {
								final MysqlConnection[] con=new MysqlConnection[1];
								con[0]=new MysqlConnection("localhost", 3306, "root", "mysql", "async", multiplexer.getSelector(), 
										new SuccessCallback() {

									@Override
									public void onError(SQLException e) {
										try {
											con[0].close();
										} catch (SQLException e1) {

											e1.printStackTrace();
										}

									}

									@Override
									public void onSuccess(OK ok) {
										e.doInConnection(con[0]);


									}
								}
										);
							}



						} catch (IOException e) {
							e.printStackTrace();
							return;
						}
					}

				}
			});
		}
	}
	public void q(Query query) throws InterruptedException {
		q.put(query);
		for(Multiplexer mpx:mpxs) {
			mpx.getSelector().wakeup();
		}

	}

	public void stop() {
		pool.shutdown();
	}

}