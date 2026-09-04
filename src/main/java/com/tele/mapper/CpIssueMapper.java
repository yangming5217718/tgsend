package com.tele.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.tele.entity.CpIssue;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


public interface CpIssueMapper extends BaseMapper<CpIssue> {
    @Select("select * from cp_issue where room_id=#{roomId} and issue = #{issue} " +
            "and status=0 and close_status = 0 order by statustime asc limit 1 ")
    CpIssue selectByIssueAndRoomId(@Param("roomId") String roomId,@Param("issue")String issue);
}
