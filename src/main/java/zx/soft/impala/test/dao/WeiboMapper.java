package zx.soft.impala.test.dao;

import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import zx.soft.impala.test.domain.Weibo;

public interface WeiboMapper {

	@Select("SELECT MAX(id) FROM  ${tablename}")
	public int getCount(@Param("tablename") String tablename);

	@Select("SELECT name FROM sina_tablenames")
	public List<String> getTablename();

	@Select("SELECT wid,username,repostscount,commentscount,text,createat,owid,ousername,favorited,originalpic,source,"
			+ "visible,lasttime FROM ${tablename} WHERE id >= #{from} AND id<(#{from}+1000)")
	public List<Weibo> getWeiboData(@Param("tablename") String tablename, @Param("from") int from);
}
