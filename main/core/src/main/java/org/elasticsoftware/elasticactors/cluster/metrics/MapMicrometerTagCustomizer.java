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

package org.elasticsoftware.elasticactors.cluster.metrics;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.Tags;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link MicrometerTagCustomizer} which just wraps a map.
 */
public final class MapMicrometerTagCustomizer implements MicrometerTagCustomizer {

    private final ImmutableMap<String, Tags> tagsForComponent;

    public MapMicrometerTagCustomizer(@Nonnull Map<String, Tags> tagsForComponent) {
        this.tagsForComponent = ImmutableMap.copyOf(Objects.requireNonNull(tagsForComponent));
    }

    @Override
    @Nonnull
    public Tags get(@Nonnull String component) {
        return tagsForComponent.getOrDefault(component, Tags.empty());
    }
}
