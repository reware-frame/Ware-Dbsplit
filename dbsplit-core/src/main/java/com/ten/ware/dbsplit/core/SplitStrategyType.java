package com.ten.ware.dbsplit.core;

public enum SplitStrategyType {
	VERTICAL("vertical"), HORIZONTAL("horizontal");

	private String value;

	SplitStrategyType(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return value;
	}
}
