package org.elasticsoftware.elasticactors.cluster.metrics;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.Tags;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Class used to customize tags for the various metrics in the Elastic Actors framework
 */
public class MeterTagCustomizer {

    private final ImmutableMap<String, Tags> tagsForComponent;

    public MeterTagCustomizer(@Nonnull ImmutableMap<String, Tags> tagsForComponent) {
        this.tagsForComponent = Objects.requireNonNull(tagsForComponent);
    }

    @Nonnull
    public Tags get(@Nonnull String component) {
        return tagsForComponent.getOrDefault(component, Tags.empty());
    }
}
