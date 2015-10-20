package zx.soft.impala.test.core;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;

import zx.soft.hbase.api.core.HBaseClient;
import zx.soft.hbase.api.core.HConn;
import zx.soft.impala.test.utils.Constant;

import com.google.protobuf.ServiceException;

public class MainPostThread {

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException,
	ServiceException {

		HBaseClient client = new HBaseClient();
		client.createTable(Constant.sina_weibo_table_name, Constant.sina_weibo_cf);
		client.close();

		HConnection conn = HConn.getHConnection();
		SQLOperation sqlOperation = new SQLOperation();
		ThreadCore threadCore = new ThreadCore();
		List<String> tablenames = sqlOperation.getTablename();
		for (String tablename : tablenames) {
			int count = sqlOperation.getCount(tablename);
			int pageCount = count / 1000;
			for (int i = 0; i < pageCount; i++) {
				int from = i * 1000;
				threadCore.getData(conn, tablename, from);
			}
		}
		threadCore.close();
	}
}
