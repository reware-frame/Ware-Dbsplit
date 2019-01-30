package com.ten.ware.dbsplit.core.sql.util;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.ResultSetMetaData;
import com.ten.ware.dbsplit.util.reflect.ReflectionUtil;

public abstract class OrmUtil {
    private static final Logger log = LoggerFactory.getLogger(OrmUtil.class);

    /**
     * 将javabean的名称转化为table的名称
     */
    public static String javaClassName2DbTableName(String name) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < name.length(); i++) {
            if (Character.isUpperCase(name.charAt(i)) && i != 0) {
                sb.append("_");
            }

            sb.append(Character.toUpperCase(name.charAt(i)));

        }
        return sb.toString();
    }

    /**
     * java地段名称 -> table列名称
     */
    public static String javaFieldName2DbFieldName(String name) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < name.length(); i++) {
            if (Character.isUpperCase(name.charAt(i))) {
                sb.append("_");
            }

            sb.append(Character.toUpperCase(name.charAt(i)));

        }
        return sb.toString();
    }

    /**
     * 列名称 -> java字段名称
     */
    public static String dbFieldName2JavaFieldName(String name) {
        StringBuilder sb = new StringBuilder();

        boolean lower = true;
        for (int i = 0; i < name.length(); i++) {
            if (name.charAt(i) == '_') {
                lower = false;
                continue;
            }

            if (lower) {
                sb.append(Character.toLowerCase(name.charAt(i)));
            } else {
                sb.append(Character.toUpperCase(name.charAt(i)));
                lower = true;
            }

        }
        return sb.toString();
    }

    /**
     * 生成占位符
     */
    public static String generateParamPlaceholders(int count) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < count; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("?");
        }

        return sb.toString();
    }

    /**
     * 将jdbc查询结果转为bean
     */
    public static <T> T convertRow2Bean(ResultSet rs, Class<T> clazz) {
        try {
            T bean = clazz.newInstance();

            ResultSetMetaData rsmd = (ResultSetMetaData) rs.getMetaData();

            // 遍历查询列
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                int columnType = rsmd.getColumnType(i);
                String columnName = rsmd.getColumnName(i);
                // 将table列名 -> bean字段名
                String fieldName = OrmUtil
                        .dbFieldName2JavaFieldName(columnName);
                // 将bean名 -> setter名
                String setterName = ReflectionUtil
                        .fieldName2SetterName(fieldName);

                if (columnType == Types.SMALLINT) {
                    Method setter = ReflectionUtil.searchEnumSetter(clazz,
                            setterName);
                    Class<?> enumParamClazz = setter.getParameterTypes()[0];
                    Method enumParseFactoryMethod = enumParamClazz.getMethod(
                            "parse", int.class);
                    Object value = enumParseFactoryMethod.invoke(
                            enumParamClazz, rs.getInt(i));

                    setter.invoke(bean, value);
                } else {
                    Class<? extends Object> param = null;
                    Object value = null;
                    switch (columnType) {
                        case Types.VARCHAR:
                            param = String.class;
                            value = rs.getString(i);
                            break;
                        case Types.BIGINT:
                            param = long.class;
                            value = rs.getLong(i);
                            break;
                        case Types.INTEGER:
                            param = int.class;
                            value = rs.getInt(i);
                            break;
                        case Types.DATE:
                            param = Date.class;
                            value = rs.getTimestamp(i);
                            break;
                        case Types.TIMESTAMP:
                            param = Date.class;
                            value = rs.getTimestamp(i);
                            break;
                        default:
                            log.error("Dbsplit doesn't support column {} type {}.",
                                    columnName, columnType);
                            throw new Exception("Db column not supported.");
                    }

                    Method setter = clazz.getMethod(setterName, param);
                    setter.invoke(bean, value);
                }
            }

            return bean;
        } catch (Exception e) {
            log.error("Fail to operator on ResultSet metadata for clazz {}.",
                    clazz);
            log.error("Exception--->", e);
            throw new IllegalStateException(
                    "Fail to operator on ResultSet metadata.", e);
        }
    }
}
