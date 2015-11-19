package zx.soft.impala.test.query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.Format;

public class QuerySpeedTest {

	public static void main(String[] args) {
		//000000000000001042700000000012571475960000000000005840022最小key
		//000000000176037346700000000013428300920003470143277174799
		Format format = new DecimalFormat("0000000000000000000");
		String keyStart = format.format(2337893400L);
		String keyEnd = format.format(2337893401L);
		long i = 0;
		String sqlStatement = "select count(*) from sina_weibo_100million where key between \'" + keyStart
				+ "\' and \'" + keyEnd + "\'";
		//String sqlStatement = "select count(*) from parquet_compression.sina_weibo_100million_parquet where key < \'0000000000100010427\' and repostcount>100";
		//String sqlStatement = "select key from parquet_compression.sina_weibo_100million_parquet where commentscount>2000";
		long before = System.currentTimeMillis();
		try (Connection conn = ImpalaConnection.getConnection();
				Statement statement = conn.createStatement();
				ResultSet resultSet = statement.executeQuery(sqlStatement);) {
			while (resultSet.next()) {
				i++;
				System.out.println(resultSet.getString(1));
			}
			System.out.println("查询时间:" + (System.currentTimeMillis() - before));
			System.out.println(i);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
