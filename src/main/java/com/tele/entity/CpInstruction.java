package com.tele.entity;

import java.io.Serializable;

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
@TableName("cp_instruction")
public class CpInstruction implements Serializable {
    private static final long serialVersionUID = 1L;
    private int id;
    private Integer instructionType;
    private int gametype;
    private String gametypename;
    private String insname;
    private String inskey;
    private int status;
    private String pattern;
}
