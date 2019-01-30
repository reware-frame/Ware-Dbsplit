package com.ten.ware.dbsplit.core;

/**
 * 分片策略
 */
public interface SplitStrategy {
    public <K> int getNodeNo(K splitKey);

    public <K> int getDbNo(K splitKey);

    public <K> int getTableNo(K splitKey);
}
