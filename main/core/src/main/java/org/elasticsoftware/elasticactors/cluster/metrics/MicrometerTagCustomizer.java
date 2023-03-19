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

import io.micrometer.core.instrument.Tags;

import jakarta.annotation.Nonnull;

/**
 * Type used to customize tags for the various metrics in the Elastic Actors framework
 */
public interface MicrometerTagCustomizer {

    /**
     * Provides a set of tags for using with the provided component.
     * @param component the name of the component for which to get tags.
     * @return the tags for the component.
     */
    @Nonnull
    Tags get(@Nonnull String component);
}
