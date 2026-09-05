package com.tele.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.tele.entity.CpBotmessageSendInlineItem;

/**
 * <p>
 * inline 实例 Mapper
 * </p>
 *
 * @author lindark
 */
public interface CpBotmessageSendInlineItemMapper extends BaseMapper<CpBotmessageSendInlineItem> {

    /**
     * 落实例。
     * <p>
     * 同一条 chosen_inline_result 可能被 Telegram 重投，
     * 靠唯一键 uk_inline_message_id 兜幂等。
     * 这里用 INSERT IGNORE：重复时影响 0 行，不抛异常。
     */
    @Insert("""
            insert ignore into cp_botmessage_send_inline_item
                (inline_id, inline_message_id, botcode, from_id, chat_instance,
                 query_text, status, createtime, updatetime, source)
            values
                (#{inlineId}, #{inlineMessageId}, #{botcode}, #{fromId}, #{chatInstance},
                 #{queryText}, #{status}, #{createtime}, #{updatetime}, #{source})
            """)
    int insertIgnore(CpBotmessageSendInlineItem item);

    /**
     * 取某条母版下所有还有效的实例，用于批量编辑。
     */
    @Select("""
            select * from cp_botmessage_send_inline_item
            where inline_id = #{inlineId} and status = 1
            order by id asc
            """)
    List<CpBotmessageSendInlineItem> selectAliveByInlineId(@Param("inlineId") String inlineId);
}
