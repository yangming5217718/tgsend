package com.tele.entity;

import java.io.Serializable;
import java.math.BigDecimal;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Getter;
import lombok.Setter;

/**
 * <p>
 * inline 消息母版表。
 * 一条母版可以被多个用户分享出去，
 * 每次分享在 cp_botmessage_send_inline_item 里落一条实例。
 * </p>
 *
 * @author lindark
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@TableName("cp_botmessage_send_inline")
public class CpBotmessageSendInline implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 主键是业务侧生成的字符串，不是自增 */
    @TableId(value = "id", type = IdType.INPUT)
    private String id;
    private String botid;
    private String content;
    private String createtime;
    private String sendtime;
    private String locktime;
    private int status;
    private String imgsrc;
    private int failtimes;
    private String buttontext;
    private int msgtype;
    private BigDecimal moneys;
    private String returnmsg;
    private String botcode;
    private String transferid;
    /** html / markdown / markdownV2 */
    private String parsemode;
}
