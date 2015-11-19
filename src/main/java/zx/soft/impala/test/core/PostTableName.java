package zx.soft.impala.test.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import zx.soft.impala.test.dao.WeiboMapper;
import zx.soft.impala.test.utils.MybatisConfig;

public class PostTableName {
	private static final String LEI_ROUTE = "/home/lb/tablenames.txt";
	public static final int ONE_M = 1024;
	private static SqlSessionFactory sqlSessionFactory;
	
	public static void main(String[] args) {
		sqlSessionFactory = MybatisConfig.getSqlSessionFactory();
		try (SqlSession sqlSession = sqlSessionFactory.openSession(true);) {
			WeiboMapper weiboMapper = sqlSession.getMapper(WeiboMapper.class);
			try {
				String lei[] = inputTest(LEI_ROUTE);
				for (int i = 0; i < lei.length - 1; i++) {
				weiboMapper.addTableName(lei[i]);
				}
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
	private static String[] inputTest(String route) throws IOException {
		FileInputStream fin = new FileInputStream(route);
		FileChannel fc = fin.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(ONE_M);
		fc.read(buffer);
		buffer.flip();
		byte[] array = buffer.array();
		String s[] = new String(array).toString().split("  ");
		fc.close();
		fin.close();

		return s;
	}
}