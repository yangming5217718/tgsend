package com.tele.entity;

import java.io.Serializable;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

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
@TableName("cp_instruction_user")
public class CpInstructionUser implements Serializable {
    private static final long serialVersionUID = 1L;
    @TableId(value = "id", type = IdType.AUTO)
    private int id;
    private String telegramUserId;
    private String userName;
    private String userCoding;
    private int fromtype;
    private int status;
    private String createtime;
    private String chatid;
    private String msgid;
    
}
