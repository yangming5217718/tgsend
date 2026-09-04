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
 * 
 * </p>
 *
 * @author lindark
 * @since 2023-08-21
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@TableName("cp_botmessage_send_userother")
public class CpBotmessageSendUserother implements Serializable {
    private static final long serialVersionUID = 1L;
    @TableId(value = "id", type = IdType.AUTO)
    private long id;
    private String telegramUserId;
    private String content;
    private String createtime;
    private String sendtime;
    private int status;
    private String imgsrc;
    private int failtimes;
    private String buttontext;
    private int msgtype;
    private String returnmsg;
    private String chatid;
    private String msgid;
    private String exptime;
    private String insid;
}
