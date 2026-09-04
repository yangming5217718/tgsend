package com.tele.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.tele.entity.CpInstructionUser;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author lindark
 * @since 2023-08-21
 */
public interface CpInstructionUserMapper extends BaseMapper<CpInstructionUser> {

    @Select("select * from cp_instruction_user where telegram_user_id = #{telegramUserId} limit 1")
    CpInstructionUser selectByTelegramUserId(@Param("telegramUserId") String telegramUserId);
}
