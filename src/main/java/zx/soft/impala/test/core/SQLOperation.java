package zx.soft.impala.test.core;

import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.impala.test.dao.WeiboMapper;
import zx.soft.impala.test.domain.Weibo;
import zx.soft.impala.test.utils.MybatisConfig;

/**
 * 数据库操作类
 * @author fgq
 *
 */
public class SQLOperation {

	private static Logger logger = LoggerFactory.getLogger(SQLOperation.class);
	private static SqlSessionFactory sqlSessionFactory;

	public SQLOperation() {
		sqlSessionFactory = MybatisConfig.getSqlSessionFactory();
	}

	/**
	 * 获得给定表给定id范围的Weibo数据
	 * @param tablename
	 * @param id
	 * @return
	 */
	public List<Weibo> getWeiboData(String tablename, int from) {

		try (SqlSession sqlSession = sqlSessionFactory.openSession();) {
			WeiboMapper weiboMapper = sqlSession.getMapper(WeiboMapper.class);
			List<Weibo> weibos = weiboMapper.getWeiboData(tablename, from);
			logger.info("get id from " + from + " to " + (from + weibos.size()));
			return weibos;
		}
	}

	/**
	 *获得给定表的数据总数
	 * @param tablename
	 * @return
	 */
	public int getCount(String tablename) {
		try (SqlSession sqlSession = sqlSessionFactory.openSession();) {
			WeiboMapper weiboMapper = sqlSession.getMapper(WeiboMapper.class);
			int count = weiboMapper.getCount(tablename);
			logger.info("table\'s size: " + count);
			return count;
		}
	}

	/**
	 * 返回数据表sina_tablenames中保存的所有数据表名
	 * @return
	 */
	public List<String> getTablename() {
		try (SqlSession sqlSession = sqlSessionFactory.openSession();) {
			WeiboMapper weiboMapper = sqlSession.getMapper(WeiboMapper.class);
			List<String> tablename = weiboMapper.getTablename();
			logger.info("tablename size=" + tablename.size());
			return tablename;
		}
	}

	public static void main(String[] args) {
		SQLOperation sqlOperation = new SQLOperation();
		List<Weibo> weibos = sqlOperation.getWeiboData("sina_user_weibos_1386622641", 0);
		//sqlOperation.getCount("sina_user_weibos_1386614765");
		//sqlOperation.getTablename();
	}
}
