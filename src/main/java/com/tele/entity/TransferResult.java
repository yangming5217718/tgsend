package com.tele.entity;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransferResult {
	private String txid;
	private boolean success;
	private String remark;
}
