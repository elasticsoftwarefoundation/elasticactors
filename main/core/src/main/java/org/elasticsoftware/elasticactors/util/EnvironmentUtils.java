package org.elasticsoftware.elasticactors.util;

import com.google.common.collect.ImmutableMap;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;

import javax.annotation.Nonnull;
import java.util.function.Function;

public final class EnvironmentUtils {

    private EnvironmentUtils() {
    }

    @Nonnull
    public static <T> ImmutableMap<String, T> getKeyValuePairsUnderPrefix(
        @Nonnull Environment environment,
        @Nonnull String prefix,
        @Nonnull Function<String, T> converter)
    {
        if (!prefix.endsWith(".")) {
            prefix = prefix + ".";
        }
        ImmutableMap.Builder<String, T> mapBuilder = ImmutableMap.builder();
        if (environment instanceof ConfigurableEnvironment) {
            for (PropertySource<?> propertySource : ((ConfigurableEnvironment) environment).getPropertySources()) {
                if (propertySource instanceof EnumerablePropertySource) {
                    for (String key : ((EnumerablePropertySource<?>) propertySource).getPropertyNames()) {
                        if (key.length() > prefix.length() && key.startsWith(prefix)) {
                            Object property = propertySource.getProperty(key);
                            if (property != null) {
                                String value = property.toString();
                                T converted = converter.apply(value);
                                String subKey = key.substring(prefix.length());
                                mapBuilder.put(subKey, converted);
                            }
                        }
                    }
                }
            }
        }
        return mapBuilder.build();
    }
}
