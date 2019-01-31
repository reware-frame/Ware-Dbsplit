package com.ten.ware.dbsplit.core;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.ten.ware.dbsplit.core.sql.parser.SplitSqlParser;
import com.ten.ware.dbsplit.core.sql.parser.SplitSqlStructure;
import com.ten.ware.dbsplit.excep.NotSupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;

/**
 * JDBC操作API
 */
public class SplitJdbcTemplate implements SplitJdbcOperations {
    protected static final Logger log = LoggerFactory
            .getLogger(SplitJdbcTemplate.class);

    protected SplitTablesHolder splitTablesHolder;

    protected SplitActionRunner splitActionRunner = new SplitActionRunner();

    protected boolean readWriteSeparate = false;

    interface SplitAction<T> {
        T doSplitAction(JdbcTemplate jt, String sql);
    }

    /**
     * SQL分片并执行操作
     */
    class SplitActionRunner {
        /**
         * @param splitKey    切分ID
         * @param sql         通用sql
         * @param splitAction lambda委托查询
         * @param <T>         查询结果类型(对象类型)
         * @param <K>         ID类型
         * @return {@literal 查询结果类型(对象类型)}
         */
        <T, K> T runSplitAction(K splitKey, String sql,
                                SplitAction<T> splitAction) {
            log.debug("runSplitAction entry, splitKey {} sql {}", splitKey, sql);

            // 解析SQL为分片SQL
            SplitSqlStructure splitSqlStructure = SplitSqlParser.INST
                    .parseSplitSql(sql);

            String dbName = splitSqlStructure.getDbName();
            String tableName = splitSqlStructure.getTableName();

            SplitTable splitTable = splitTablesHolder.searchSplitTable(dbName,
                    tableName);

            SplitStrategy splitStrategy = splitTable.getSplitStrategy();

            int nodeNo = splitStrategy.getNodeNo(splitKey);
            int dbNo = splitStrategy.getDbNo(splitKey);
            int tableNo = splitStrategy.getTableNo(splitKey);

            List<SplitNode> splitNodes = splitTable.getSplitNodes();

            SplitNode sn = splitNodes.get(nodeNo);
            JdbcTemplate jt = getJdbcTemplate(sn, false);

            sql = splitSqlStructure.getSplitSql(dbNo, tableNo);

            log.debug(
                    "runSplitAction do action, splitKey {} sql {} dbName {} tableName {} nodeNo {} dbNo {} tableNo {}",
                    splitKey, sql, dbName, tableName, nodeNo, dbNo, tableNo);
            T result = splitAction.doSplitAction(jt, sql);

            log.debug(
                    "runSplitAction return, {} are returned, splitKey {} sql {}",
                    result, splitKey, sql);
            return result;
        }
    }

    public SplitJdbcTemplate() {

    }

    public SplitJdbcTemplate(SplitTablesHolder splitTablesHolder) {
        this.splitTablesHolder = splitTablesHolder;
    }

    public SplitJdbcTemplate(List<String> ipPorts, String user,
                             String password, String... tables) {
        this.addTable(ipPorts, user, password, tables);
    }

    public void addTable(List<String> ipPorts, String user, String password,
                         String... tables) {
        // TODO parse datasources and tables
    }

    @Override
    public <T, K> T execute(K splitKey, ConnectionCallback<T> action)
            throws DataAccessException {
        throw new NotSupportedException();
    }

    @Override
    public <T, K> T execute(K splitKey, StatementCallback<T> action)
            throws DataAccessException {
        throw new NotSupportedException();
    }

    @Override
    public <K> void execute(K splitKey, String sql) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <T, K> T query(K splitKey, String sql, ResultSetExtractor<T> rse)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T result = jt.query(sql1, rse);

                    return result;
                });
    }

    public <K> void query(K splitKey, String sql, RowCallbackHandler rch)
            throws DataAccessException {
        splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    jt.query(sql1, rch);

                    return null;
                });
    }

    public <T, K> List<T> query(K splitKey, String sql, RowMapper<T> rowMapper)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<T> result = jt.query(sql1, rowMapper);

                    return result;
                });
    }

    public <T, K> T queryForObject(K splitKey, String sql,
                                   RowMapper<T> rowMapper) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T result = jt.queryForObject(sql1, rowMapper);

                    return result;
                });
    }

    public <T, K> T queryForObject(K splitKey, String sql, Class<T> requiredType)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T result = jt.queryForObject(sql1, requiredType);

                    return result;
                });
    }

    public <K> Map<String, Object> queryForMap(K splitKey, String sql)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    Map<String, Object> result = jt.queryForMap(sql1);

                    return result;
                });
    }

    public <T, K> List<T> queryForList(K splitKey, String sql,
                                       Class<T> elementType) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<T> result = jt.queryForList(sql1, elementType);

                    return result;
                });
    }

    public <K> List<Map<String, Object>> queryForList(K splitKey, String sql)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<Map<String, Object>> result = jt.queryForList(sql1);

                    return result;
                });
    }

    public <K> SqlRowSet queryForRowSet(K splitKey, String sql)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    SqlRowSet result = jt.queryForRowSet(sql1);

                    return result;
                });
    }

    public <K> int update(K splitKey, String sql) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <K> int[] batchUpdate(K splitKey, String... sql)
            throws DataAccessException {
        throw new NotSupportedException();
    }

    public <T, K> T execute(K splitKey, PreparedStatementCreator psc,
                            PreparedStatementCallback<T> action) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <T, K> T execute(K splitKey, String sql,
                            PreparedStatementCallback<T> action) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <T, K> T query(K splitKey, PreparedStatementCreator psc,
                          ResultSetExtractor<T> rse) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <T, K> T query(K splitKey, String sql, PreparedStatementSetter pss,
                          ResultSetExtractor<T> rse) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T result = jt.query(sql1, pss, rse);

                    return result;
                });
    }

    public <T, K> T query(K splitKey, String sql, Object[] args,
                          int[] argTypes, ResultSetExtractor<T> rse)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T result = jt.query(sql1, args, argTypes, rse);

                    return result;
                });
    }

    public <T, K> T query(K splitKey, String sql, Object[] args,
                          ResultSetExtractor<T> rse) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T result = jt.query(sql1, args, rse);

                    return result;
                });
    }

    public <T, K> T query(K splitKey, String sql, ResultSetExtractor<T> rse,
                          Object... args) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T result = jt.query(sql1, rse, args);

                    return result;
                });
    }

    public <K> void query(K splitKey, PreparedStatementCreator psc,
                          RowCallbackHandler rch) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <K> void query(K splitKey, String sql, PreparedStatementSetter pss,
                          RowCallbackHandler rch) throws DataAccessException {
        splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    jt.query(sql1, pss, rch);

                    return null;
                });
    }

    public <K> void query(K splitKey, String sql, Object[] args,
                          int[] argTypes, RowCallbackHandler rch) throws DataAccessException {
        splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    jt.query(sql1, args, argTypes, rch);

                    return null;
                });
    }

    public <K> void query(K splitKey, String sql, Object[] args,
                          RowCallbackHandler rch) throws DataAccessException {
        splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    jt.query(sql1, args, rch);

                    return null;
                });
    }

    public <K> void query(K splitKey, String sql, RowCallbackHandler rch,
                          Object... args) throws DataAccessException {
        splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    jt.query(sql1, rch, args);

                    return null;
                });
    }

    public <T, K> List<T> query(K splitKey, PreparedStatementCreator psc,
                                RowMapper<T> rowMapper) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <T, K> List<T> query(K splitKey, String sql,
                                PreparedStatementSetter pss, RowMapper<T> rowMapper)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<T> ret = jt.query(sql1, pss, rowMapper);
                    return ret;
                });
    }

    public <T, K> List<T> query(K splitKey, String sql, Object[] args,
                                int[] argTypes, RowMapper<T> rowMapper) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<T> ret = jt.query(sql1, args, argTypes, rowMapper);
                    return ret;
                });
    }

    public <T, K> List<T> query(K splitKey, String sql, Object[] args,
                                RowMapper<T> rowMapper) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<T> ret = jt.query(sql1, args, rowMapper);
                    return ret;
                });
    }

    public <T, K> List<T> query(K splitKey, String sql, RowMapper<T> rowMapper,
                                Object... args) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<T> ret = jt.query(sql1, rowMapper, args);
                    return ret;
                });
    }

    public <T, K> T queryForObject(K splitKey, String sql, Object[] args,
                                   int[] argTypes, RowMapper<T> rowMapper) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T ret = jt.queryForObject(sql1, args, argTypes,
                            rowMapper);
                    return ret;
                });
    }

    @Override
    public <T, K> T queryForObject(K splitKey, String sql, Object[] args,
                                   RowMapper<T> rowMapper) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T ret = jt.queryForObject(sql1, args, rowMapper);
                    return ret;
                });
    }

    public <T, K> T queryForObject(K splitKey, String sql,
                                   RowMapper<T> rowMapper, Object... args) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T ret = jt.queryForObject(sql1, rowMapper, args);
                    return ret;
                });
    }

    public <T, K> T queryForObject(K splitKey, String sql, Object[] args,
                                   int[] argTypes, Class<T> requiredType) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T ret = jt.queryForObject(sql1, args, argTypes,
                            requiredType);
                    return ret;
                });
    }

    public <T, K> T queryForObject(K splitKey, String sql, Object[] args,
                                   Class<T> requiredType) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T ret = jt.queryForObject(sql1, args, requiredType);
                    return ret;
                });
    }

    public <T, K> T queryForObject(K splitKey, String sql,
                                   Class<T> requiredType, Object... args) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    T ret = jt.queryForObject(sql1, requiredType, args);
                    return ret;
                });
    }

    public <K> Map<String, Object> queryForMap(K splitKey, String sql,
                                               Object[] args, int[] argTypes) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    Map<String, Object> ret = jt.queryForMap(sql1, args,
                            argTypes);
                    return ret;
                });
    }

    public <K> Map<String, Object> queryForMap(K splitKey, String sql,
                                               Object... args) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    Map<String, Object> ret = jt.queryForMap(sql1, args);
                    return ret;
                });
    }

    public <T, K> List<T> queryForList(K splitKey, String sql, Object[] args,
                                       int[] argTypes, Class<T> elementType) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<T> ret = jt.queryForList(sql1, args, argTypes,
                            elementType);
                    return ret;
                });
    }

    public <T, K> List<T> queryForList(K splitKey, String sql, Object[] args,
                                       Class<T> elementType) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<T> ret = jt.queryForList(sql1, args, elementType);
                    return ret;
                });
    }

    public <T, K> List<T> queryForList(K splitKey, String sql,
                                       Class<T> elementType, Object... args) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<T> ret = jt.queryForList(sql1, elementType, args);
                    return ret;
                });
    }

    public <K> List<Map<String, Object>> queryForList(K splitKey, String sql,
                                                      Object[] args, int[] argTypes) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<Map<String, Object>> ret = jt.queryForList(sql1,
                            args, argTypes);
                    return ret;
                });
    }

    public <K> List<Map<String, Object>> queryForList(K splitKey, String sql,
                                                      Object... args) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    List<Map<String, Object>> ret = jt.queryForList(sql1,
                            args);
                    return ret;
                });
    }

    public <K> SqlRowSet queryForRowSet(K splitKey, String sql, Object[] args,
                                        int[] argTypes) throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    SqlRowSet ret = jt.queryForRowSet(sql1, args, argTypes);
                    return ret;
                });

    }

    public <K> SqlRowSet queryForRowSet(K splitKey, String sql, Object... args)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    SqlRowSet ret = jt.queryForRowSet(sql1, args);
                    return ret;
                });
    }

    public <K> int update(K splitKey, PreparedStatementCreator psc)
            throws DataAccessException {
        throw new NotSupportedException();
    }

    public <K> int update(K splitKey, PreparedStatementCreator psc,
                          KeyHolder generatedKeyHolder) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <K> int update(K splitKey, String sql, PreparedStatementSetter pss)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    Integer ret = jt.update(sql1, pss);
                    return ret;
                });
    }

    public <K> int update(K splitKey, String sql, Object[] args, int[] argTypes)
            throws DataAccessException {
        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    Integer ret = jt.update(sql1, args, argTypes);
                    return ret;
                });
    }

    @Override
    public <K> int update(K splitKey, String sql, Object... args)
            throws DataAccessException {

        return splitActionRunner.runSplitAction(splitKey, sql,
                (jt, sql1) -> {
                    Integer ret = jt.update(sql1, args);
                    return ret;
                });

    }

    public <K> int[] batchUpdate(K splitKey, String sql,
                                 BatchPreparedStatementSetter pss) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <K> int[] batchUpdate(K splitKey, String sql,
                                 List<Object[]> batchArgs) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <K> int[] batchUpdate(K splitKey, String sql,
                                 List<Object[]> batchArgs, int[] argTypes)
            throws DataAccessException {
        throw new NotSupportedException();
    }

    public <T, K> int[][] batchUpdate(K splitKey, String sql,
                                      Collection<T> batchArgs, int batchSize,
                                      ParameterizedPreparedStatementSetter<T> pss)
            throws DataAccessException {
        throw new NotSupportedException();
    }

    public <T, K> T execute(K splitKey, CallableStatementCreator csc,
                            CallableStatementCallback<T> action) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <T, K> T execute(K splitKey, String callString,
                            CallableStatementCallback<T> action) throws DataAccessException {
        throw new NotSupportedException();
    }

    public <K> Map<String, Object> call(K splitKey,
                                        CallableStatementCreator csc, List<SqlParameter> declaredParameters)
            throws DataAccessException {
        throw new NotSupportedException();
    }

    public SplitTablesHolder getSplitTablesHolder() {
        return splitTablesHolder;
    }

    public void setSplitTablesHolder(SplitTablesHolder splitTablesHolder) {
        this.splitTablesHolder = splitTablesHolder;
    }

    public boolean isReadWriteSeparate() {
        return readWriteSeparate;
    }

    public void setReadWriteSeparate(boolean readWriteSeparate) {
        this.readWriteSeparate = readWriteSeparate;
    }

    protected JdbcTemplate getWriteJdbcTemplate(SplitNode sn) {
        return getJdbcTemplate(sn, false);
    }

    protected JdbcTemplate getReadJdbcTemplate(SplitNode sn) {
        return getJdbcTemplate(sn, true);
    }

    protected JdbcTemplate getJdbcTemplate(SplitNode sn, boolean read) {
        if (!read)
            return sn.getMasterTemplate();

        if (readWriteSeparate)
            return sn.getRoundRobinSlaveTempate();

        return sn.getMasterTemplate();
    }
}
