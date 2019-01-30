package com.robert.dbsplit.single;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.robert.dbsplit.core.sql.util.OrmUtil;
import com.robert.dbsplit.core.sql.util.SqlUtil;
import com.robert.dbsplit.core.sql.util.SqlUtil.SqlRunningBean;

/**
 * 封装SpringJDBC template，对外提供API
 */
public class SimpleJdbcTemplate extends JdbcTemplate implements
        SimpleJdbcOperations {

    private static final Logger log = LoggerFactory
            .getLogger(SimpleJdbcTemplate.class);

    @Override
    public <T> void insert(T bean) {
        SqlRunningBean srb = SqlUtil.generateInsertSql(bean);

        log.debug("Insert, the bean: {} ---> the SQL: {} ---> the params: {}",
                bean, srb.getSql(), srb.getParams());

        this.update(srb.getSql(), srb.getParams());
    }

    @Override
    public <T> void update(T bean) {
        SqlRunningBean srb = SqlUtil.generateUpdateSql(bean);

        log.debug("Update, the bean: {} ---> the SQL: {} ---> the params: {}",
                bean, srb.getSql(), srb.getParams());

        this.update(srb.getSql(), srb.getParams());
    }

    @Override
    public <T> void delete(long id, Class<T> clazz) {
        SqlRunningBean srb = SqlUtil.generateDeleteSql(id, clazz);

        log.debug("Delete, the bean: {} ---> the SQL: {} ---> the params: {}",
                id, srb.getSql(), srb.getParams());

        this.update(srb.getSql(), srb.getParams());
    }

    @Override
    public <T> T get(long id, final Class<T> clazz) {
        SqlRunningBean srb = SqlUtil.generateSelectSql("id", id, clazz);

        T bean = this.queryForObject(srb.getSql(), srb.getParams(),
                (rs, rowNum) -> OrmUtil.convertRow2Bean(rs, clazz));
        return bean;
    }

    @Override
    public <T> T get(String name, Object value, final Class<T> clazz) {
        SqlRunningBean srb = SqlUtil.generateSelectSql(name, value, clazz);

        T bean = this.queryForObject(srb.getSql(), srb.getParams(),
                (rs, rowNum) -> OrmUtil.convertRow2Bean(rs, clazz));
        return bean;
    }

    @Override
    public <T> List<T> search(final T bean) {
        SqlRunningBean srb = SqlUtil.generateSearchSql(bean);

        List<T> beans = this.query(srb.getSql(), srb.getParams(),
                (rs, rowNum) -> (T) OrmUtil.convertRow2Bean(rs, bean.getClass()));
        return beans;
    }

    @Override
    public <T> List<T> search(String sql, Object[] params, final Class<T> clazz) {
        List<T> beans = this.query(sql, params, (rs, rowNum) -> OrmUtil.convertRow2Bean(rs, clazz));
        return beans;
    }
}
