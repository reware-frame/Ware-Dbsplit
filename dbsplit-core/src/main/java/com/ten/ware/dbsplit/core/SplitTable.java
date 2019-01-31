package com.ten.ware.dbsplit.core;

import java.util.List;

/**
 * 包含了这个表的分片有多少个数据库和表的信息
 */
public class SplitTable {
    private String dbNamePrefix;
    private String tableNamePrefix;

    private int dbNum;
    private int tableNum;

    /**
     * 切分类型，默认垂直切分
     */
    private SplitStrategyType splitStrategyType = SplitStrategyType.VERTICAL;
    /**
     * 分片策略
     */
    private SplitStrategy splitStrategy;
    /**
     * 节点结合
     */
    private List<SplitNode> splitNodes;

    /**
     * 读写分离，默认开启
     */
    private boolean readWriteSeparate = true;

    public void init() {
        // 垂直切分
        if (splitStrategyType == SplitStrategyType.VERTICAL) {
            this.splitStrategy = new VerticalHashSplitStrategy(splitNodes.size(), dbNum, tableNum);
        }
        // 水平切分
        else if (splitStrategyType == SplitStrategyType.HORIZONTAL) {
            this.splitStrategy = new HorizontalHashSplitStrategy(splitNodes.size(), dbNum, tableNum);
        }
    }

    public void setSplitStrategyType(String splitStrategyType) {
        this.splitStrategyType = SplitStrategyType.valueOf(splitStrategyType);
    }

    public String getDbNamePrefix() {
        return dbNamePrefix;
    }

    public void setDbNamePrefix(String dbNamePrifix) {
        this.dbNamePrefix = dbNamePrifix;
    }

    public String getTableNamePrefix() {
        return tableNamePrefix;
    }

    public void setTableNamePrefix(String tableNamePrifix) {
        this.tableNamePrefix = tableNamePrifix;
    }

    public int getDbNum() {
        return dbNum;
    }

    public void setDbNum(int dbNum) {
        this.dbNum = dbNum;
    }

    public int getTableNum() {
        return tableNum;
    }

    public void setTableNum(int tableNum) {
        this.tableNum = tableNum;
    }

    public List<SplitNode> getSplitNodes() {
        return splitNodes;
    }

    public void setSplitNodes(List<SplitNode> splitNodes) {
        this.splitNodes = splitNodes;
    }

    public SplitStrategy getSplitStrategy() {
        return splitStrategy;
    }

    public void setSplitStrategy(SplitStrategy splitStrategy) {
        this.splitStrategy = splitStrategy;
    }

    public boolean isReadWriteSeparate() {
        return readWriteSeparate;
    }

    public void setReadWriteSeparate(boolean readWriteSeparate) {
        this.readWriteSeparate = readWriteSeparate;
    }
}
