package zx.soft.impala.test.core;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.List;

import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.hbase.api.core.HBaseTable;
import zx.soft.impala.test.domain.Weibo;
import zx.soft.impala.test.utils.Constant;

public class GetWeiboDataThread implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(GetWeiboDataThread.class);
	private int from;
	private String mysqlTablename;
	private HConnection conn;
	public static Format format = new DecimalFormat("0000000000000000000");

	public GetWeiboDataThread(HConnection conn, String mysqlTablename, int from) {
		this.conn = conn;
		this.from = from;
		this.mysqlTablename = mysqlTablename;
	}

	@Override
	public void run() {
		try {
			SQLOperation sqlOperation = new SQLOperation();
			List<Weibo> weibos = sqlOperation.getWeiboData(mysqlTablename, from);
			HBaseTable hbaseTable = new HBaseTable(conn, Constant.sina_weibo_table_name);
			for (Weibo weibo : weibos) {
				String keyWord = format.format(weibo.getUsername()) + format.format(weibo.getCreateat())
						+ format.format(weibo.getWid());
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "us", String.valueOf(weibo.getUsername()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "re", String.valueOf(weibo.getRepostscount()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "co", String.valueOf(weibo.getCommentscount()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "te", String.valueOf(weibo.getText()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "cr", String.valueOf(weibo.getCreateat()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "ow", String.valueOf(weibo.getOwid()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "ou", String.valueOf(weibo.getOusername()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "fa", String.valueOf(weibo.isFavorited()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "or", String.valueOf(weibo.getOriginalpic()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "so", String.valueOf(weibo.getSource()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "vi", String.valueOf(weibo.getVisible()));
				hbaseTable.put(keyWord, Constant.sina_weibo_cf, "la", String.valueOf(weibo.getLasttime()));
			}
			hbaseTable.execute();
			hbaseTable.close();
			logger.info("succeed get weibo data size=" + weibos.size());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}
