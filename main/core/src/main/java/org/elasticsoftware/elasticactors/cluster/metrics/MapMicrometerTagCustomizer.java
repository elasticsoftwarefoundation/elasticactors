package org.elasticsoftware.elasticactors.cluster.metrics;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.Tags;

import javax.annotation.Nonnull;
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
