package com.ten.ware.dbsplit.core.sql.parser;

/**
 * 解析输入的SQL，找到SQL必要组成部分，并存储到一个对象中
 */
public interface SplitSqlParser {
    public static final SplitSqlParser INST = new SplitSqlParserDefImpl();

    public SplitSqlStructure parseSplitSql(String sql);
}
