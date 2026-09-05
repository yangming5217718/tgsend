package com.tele.entity;

import java.io.Serializable;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Getter;
import lombok.Setter;

/**
 * <p>
 * inline 消息发送实例表。
 * 一次分享 = 一行，inline_message_id 是编辑这条消息的唯一凭据。
 * </p>
 *
 * @author lindark
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@TableName("cp_botmessage_send_inline_item")
public class CpBotmessageSendInlineItem implements Serializable {
    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Long id;
    /** 对应 cp_botmessage_send_inline.id */
    private String inlineId;
    /** Telegram inline_message_id，唯一键 */
    private String inlineMessageId;
    private String botcode;
    private String fromId;
    private String chatInstance;
    private String queryText;
    /** 1=有效，-1=失效（消息已被删除或无法编辑） */
    private Integer status;
    private String createtime;
    private String updatetime;
    /** 来源：其它0；信用卡分享1；话费充值分享2；购买星币分享3；开通会员分享4 */
    private Integer source;
}
