package com.ten.ware.dbsplit.util.reflect;

import java.lang.reflect.Field;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 访问者设计模式
 */
public class FieldVisitor<T> {
    private static final Logger log = LoggerFactory
            .getLogger(FieldVisitor.class);

    private T bean;

    public FieldVisitor(T bean) {
        this.bean = bean;
    }

    /**
     * TODO 传入接口实现类
     */
    public void visit(FieldHandler fieldHandler) {
        List<Field> fields = ReflectionUtil.getClassEffectiveFields(bean.getClass());

        int count = 0;
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);

            Object value;
            try {
                boolean access = field.isAccessible();

                field.setAccessible(true);
                value = field.get(bean);

                if (value != null) {
                    if (value instanceof Number
                            && ((Number) value).doubleValue() == -1d) {
                        continue;
                    }

                    if (value instanceof List) {
                        continue;
                    }

                    // 传入参数，然后调用传入的lambda方法
                    fieldHandler.handle(count++, field, value);
                }

                field.setAccessible(access);
            } catch (IllegalArgumentException e) {
                log.error("Fail to obtain bean {} property {}.", bean, field);
                log.error("Exception--->", e);
            } catch (IllegalAccessException e) {
                log.error("Fail to obtain bean {} property {}.", bean, field);
                log.error("Exception--->", e);
            }
        }
    }
}
