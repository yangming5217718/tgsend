package com.tele.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.tele.entity.CpConfig;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

public interface CpConfigMapper extends BaseMapper<CpConfig> {

    @Select("select * from cp_config c where c.code=#{code} and c.status=1 ")
    CpConfig selectConfig(@Param("code") String code);

}
