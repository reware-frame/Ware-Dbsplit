package com.ten.ware.dbsplit.util.reflect;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * 公共反射方法
 */
public abstract class ReflectionUtil {
    private static final Logger log = LoggerFactory.getLogger(ReflectionUtil.class);

    /**
     * 获取类域
     */
    public static List<Field> getClassEffectiveFields(Class<?> clazz) {
        List<Field> effectiveFields = new LinkedList<>();

        while (clazz != null) {
            // 获取Fields
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (!field.isAccessible()) {
                    try {
                        // 获取getter方法
                        Method method = clazz.getMethod(fieldName2GetterName(field.getName()));

                        // 类型检查
                        if (method.getReturnType() != field.getType()) {
                            log.error(
                                    "The getter for field {} may not be correct.",
                                    field);
                            continue;
                        }
                    } catch (NoSuchMethodException | SecurityException e) {
                        log.error(
                                "Fail to obtain getter method for non-accessible field {}.",
                                field);
                        log.error("Exception--->", e);

                        continue;
                    }

                }
                // 添加到list
                effectiveFields.add(field);
            }
            clazz = clazz.getSuperclass();
        }
        return effectiveFields;
    }

    public static String fieldName2GetterName(String fieldName) {
        return "get" + StringUtils.capitalize(fieldName);
    }

    public static String fieldName2SetterName(String fieldName) {
        return "set" + StringUtils.capitalize(fieldName);
    }

    /**
     * 获取域的值
     */
    public static <T> Object getFieldValue(T bean, String fieldName) {
        Field field;
        try {
            field = bean.getClass().getDeclaredField(fieldName);
        } catch (NoSuchFieldException | SecurityException e) {
            log.error("Fail to obtain field {} from bean {}.", fieldName, bean);
            log.error("Exception--->", e);
            throw new IllegalStateException("Refelction error: ", e);
        }

        boolean access = field.isAccessible();
        field.setAccessible(true);

        Object result = null;
        try {
            result = field.get(bean);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            log.error("Fail to obtain field {}'s value from bean {}.",
                    fieldName, bean);
            log.error("Exception--->", e);
            throw new IllegalStateException("Refelction error: ", e);
        }
        field.setAccessible(access);

        return result;
    }

    public static Method searchEnumSetter(Class<?> clazz, String methodName) {
        Method[] methods = clazz.getMethods();

        for (Method method : methods) {
            if (method.getName().equals(methodName)) {
                if (method.getParameterCount() > 0) {
                    Class<?> paramType = method.getParameterTypes()[0];
                    if (Enum.class.isAssignableFrom(paramType)) {
                        return method;
                    }
                }
            }
        }

        return null;
    }
}
