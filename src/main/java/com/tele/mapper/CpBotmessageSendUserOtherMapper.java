package com.tele.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Select;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.tele.entity.CpBotmessageSendUserother;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author lindark
 * @since 2023-08-21
 */
public interface CpBotmessageSendUserOtherMapper extends BaseMapper<CpBotmessageSendUserother> {
	@Select({"select t.* from cp_botmessage_send_userother t where t.status=0 order by  t.msgtype,t.id"})
	List<CpBotmessageSendUserother>  selectMsgListForUser();
}
