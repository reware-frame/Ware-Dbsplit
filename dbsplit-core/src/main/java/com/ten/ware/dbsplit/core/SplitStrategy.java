package com.ten.ware.dbsplit.core;

/**
 * 分片策略
 */
public interface SplitStrategy {
    <K> int getNodeNo(K splitKey);

    <K> int getDbNo(K splitKey);

    <K> int getTableNo(K splitKey);
}
