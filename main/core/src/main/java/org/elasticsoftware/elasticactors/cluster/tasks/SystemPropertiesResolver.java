package org.elasticsoftware.elasticactors.cluster.tasks;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

public final class SystemPropertiesResolver {

    private final static ConversionService conversionService = new DefaultConversionService();

    public static <T> T getProperty(String key, Class<T> targetType) {
        String value = System.getProperty(key);
        if (value != null) {
            if (conversionService.canConvert(value.getClass(), targetType)) {
                return conversionService.convert(value, targetType);
            }
        }
        return null;
    }

    public static <T> T getProperty(String key, Class<T> targetType, T defaultValue) {
        T value = getProperty(key, targetType);
        return (value != null) ? value : defaultValue;
    }

    public static <T> T getRequiredProperty(String key, Class<T> targetType)
        throws IllegalStateException
    {
        T value = getProperty(key, targetType);
        if (value == null) {
            throw new IllegalStateException(String.format("Required key [%s] not found", key));
        }
        return value;
    }

}
