package com.tele.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@TableName("cp_game_room")
public class CpGameRoom {

    private String id;
    private String gamecode;
    private String type;
    private String typename;
    private String roomName;
    private String chatId;
    private String bankerTelegramId;
    private String guaranteeTelegramId;
    private String otherChatId;
    private String walletApiUrl;
    private String walletAgentId;
    private String walletAgentPrivateKey;
    private String walletPlatformPublicKey;
    private String walletAgentPublicKey;
    private String walletRobotAddress;
    private String walletCallbackUrl;
    private String issuePrefix;
    private Long issueSeq;
    private Integer intervalSecond;
    private Integer closeSecond;
    private String startImage;
    private String closeImage;
    private String openAwardResultQuickMenu;
    private String userBetQuickMenu;
    private String startBetQuickMenu;
    private String commandOperator;
    private String startConfigText;
    private String backBetMin;
    private String backConfig;
    private String bankerPercentage;
    private String guaranteePercentage;
    private BigDecimal maxBet;
    private BigDecimal maxPayMoney;
    private Integer status;
    private String createUserId;
    private String createTime;
    private String updateTime;




}
