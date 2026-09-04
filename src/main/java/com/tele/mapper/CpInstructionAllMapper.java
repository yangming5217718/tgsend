package com.tele.mapper;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.tele.entity.CpInstructionAll;
import com.tele.entity.CpUser;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author lindark
 * @since 2023-08-21
 */
public interface CpInstructionAllMapper extends BaseMapper<CpInstructionAll> {
	
	
	@Select("SELECT * FROM cp_user WHERE botid = #{botid}")
	CpUser getBalance(@Param("botid") String botid);
	
	@Select("SELECT  SUM(g.CellScore) AS cellScore, SUM(g.Profit) AS profit, max(t.moneys) as moneys FROM cp_gamerec_all g INNER JOIN cp_user t ON t.id = g.userid WHERE g.today = #{today} AND t.botid = #{botid} GROUP BY g.userid ")
	    Map<String, Object>getFLow(
	        @Param("today") String today,
	        @Param("botid") String botid
	    );
	
	@Select("SELECT b.* FROM cp_buys b INNER JOIN cp_user t ON t.id = b.userid WHERE t.botid = #{botid} ORDER BY b.createtime DESC LIMIT 5")
		List<Map<String, Object>> getBuyList(@Param("botid") String botid);
	
	

}
