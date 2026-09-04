package com.tele.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@TableName("cp_issue")
public class CpIssue {
    private String id;
    private String roomId;
    private String chatId;
    private Integer type;
    private String typename;
    private String issue;
    private String opennum;
    private Integer status;
    private Integer closeStatus;
    private String endtime;
    private String statustime;
    private Integer paystatus;
    private String paytime;
    private String createtime;
    private String remark;

}
