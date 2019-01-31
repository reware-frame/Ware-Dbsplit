package com.ten.ware.dbsplit.core;

/**
 * 切分类型
 */
public enum SplitStrategyType {
    /**
     * 垂直切分, 水平切分
     */
    VERTICAL("vertical"),
    HORIZONTAL("horizontal");

    private String value;

    SplitStrategyType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
