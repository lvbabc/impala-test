package zx.soft.impala.test.core;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.threads.ApplyThreadPool;

public class ThreadCore {

	private static Logger logger = LoggerFactory.getLogger(ThreadCore.class);
	private static ThreadPoolExecutor pool;

	public ThreadCore() {
		pool = ApplyThreadPool.getThreadPoolExector(4);
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				pool.shutdown();
				logger.info("pool is shutdown");
			}
		}));
	}

	public void getData(HConnection conn, String tablename, int from) {
		if (!pool.isShutdown()) {
			try {
				pool.execute(new GetWeiboDataThread(conn, tablename, from));
			} catch (Exception e) {
				logger.error("pool executor error");
				throw new RuntimeException();
			}
		}
	}

	public void close() {
		pool.shutdown();
		try {
			pool.awaitTermination(20, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
			throw new RuntimeException();
		}
	}

}
