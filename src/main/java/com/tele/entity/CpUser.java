package com.tele.entity;

import java.io.Serializable;
import java.math.BigDecimal;

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
@TableName("cp_user")
public class CpUser implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;

    private Integer ucode;

    private String botid;

    private Integer upucode;
    
    private String uhashid;

    private BigDecimal moneys;
    
    private String createtime;
    
    private Integer version;
    
    private String uname;
    
    private String uanum;
    
    private String levelcode;
    
    private int istelmember;
    
    private int newwaterstatus;
    private int agent;
    
    private int isnewuserhd;
    
    private int isoktg;
    
    private String lang;
    
    private String ads;
    
    
    
}
