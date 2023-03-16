/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

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
                        if (key.startsWith(prefix)) {
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
