package com.tele.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.tele.entity.CpBotmessageSendUser;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author lindark
 * @since 2023-08-21
 */
public interface CpBotmessageSendUserMapper extends BaseMapper<CpBotmessageSendUser> {
	@Select({"select t.* from cp_botmessage_send_user t where t.status = 0 order by t.msgtype asc, t.id asc limit 500"})
	List<CpBotmessageSendUser>  selectMsgListForUser();

	/**
	 * 按「发出去之后拿到的 message_id」回查原始行，给编辑路径取 parsemode 和 buttontext 用。
	 * <p>
	 * <b>条件里是 sendid，不是 msgid。</b>这张表上两个列都叫「消息 id」，语义正相反：
	 * <ul>
	 *   <li>{@code sendid} —— 本条发送成功后 Telegram 返回的 message_id，由 updateMainAck 写入</li>
	 *   <li>{@code msgid} —— <b>回复目标</b>的 message_id，发送时传给 setReplyToMessageId</li>
	 * </ul>
	 * 查错列不会报错，只会永远查不到、一路走兜底分支，看起来一切正常。
	 * <p>
	 * 不加 {@code status = 1}：有 sendid 就说明发成功过，行后来被别的流程改了状态，
	 * 我们要的两个字段照样有效。
	 * <p>
	 * {@code order by id desc limit 1} 是防御性的——(chatid, sendid) 理论上唯一，
	 * 但没有唯一约束保证，真出现重复时取最新一条比抛异常好。
	 * <p>
	 * 走索引 {@code idx_chatid_sendid (chatid, sendid)}。
	 */
	@Select({"select t.* from cp_botmessage_send_user t",
			"where t.chatid = #{chatid} and t.sendid = #{sendid}",
			"order by t.id desc limit 1"})
	CpBotmessageSendUser selectByChatIdAndSendId(@Param("chatid") String chatid,
	                                             @Param("sendid") String sendid);
}
