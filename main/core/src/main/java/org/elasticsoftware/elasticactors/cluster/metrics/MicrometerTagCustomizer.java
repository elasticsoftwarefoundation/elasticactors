package org.elasticsoftware.elasticactors.cluster.metrics;

import io.micrometer.core.instrument.Tags;

import javax.annotation.Nonnull;

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
