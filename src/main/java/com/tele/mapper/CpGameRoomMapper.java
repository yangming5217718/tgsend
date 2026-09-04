package com.tele.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.tele.entity.CpGameRoom;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

public interface CpGameRoomMapper extends BaseMapper<CpGameRoom> {
    @Select("select * from cp_game_room where chat_id=#{chatId} and status=1 ")
    CpGameRoom selectByChatId(@Param("chatId") String chatId);
}
