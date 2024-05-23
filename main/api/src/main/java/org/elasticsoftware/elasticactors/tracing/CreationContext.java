/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.tracing;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.StringJoiner;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;

public final class CreationContext {

    private final String creator;
    private final String creatorType;
    private final String creatorMethod;
    private final Boolean scheduled;

    @Nullable
    public static CreationContext forScheduling(@Nullable CreationContext original) {
        if (original != null) {
            return new CreationContext(
                    original.getCreator(),
                    original.getCreatorType(),
                    original.getCreatorMethod(),
                    true);
        }
        return null;
    }

    /**
     * Creates a new CreationContext. The strings provided are not shortened. For external usage,
     * it's highly recommended to use {@link CreationContext#CreationContext(String, Class, Method)}
     * instead.
     *
     * <br>
     * If you use this method, however, be sure to provide short strings as to not overload the log
     * aggregator and keep the message sizes small.
     *
     * <br>
     * See {@link TracingUtils#shorten(String)}.
     *
     * @param creator the creator of this context
     * @param creatorType the type of the creator of this context
     * @param creatorMethod the method on which this context was created
     */
    public CreationContext(
            @Nullable String creator,
            @Nullable String creatorType,
            @Nullable String creatorMethod) {
        this(creator, creatorType, creatorMethod, null);
    }

    /**
     * Creates a new CreationContext. The strings provided are not shortened. For external usage,
     * it's highly recommended to use {@link CreationContext#CreationContext(String, Class, Method)}
     * instead.
     *
     * <br>
     * If you use this method, however, be sure to provide short strings as to not overload the log
     * aggregator and keep the message sizes small.
     *
     * <br>
     * See {@link TracingUtils#shorten(String)}.
     *
     * @param creator the creator of this context
     * @param creatorType the type of the creator of this context
     * @param creatorMethod the method on which this context was created
     */
    public CreationContext(
            @Nullable String creator,
            @Nullable String creatorType,
            @Nullable Method creatorMethod) {
        this(creator, creatorType, shorten(creatorMethod), null);
    }

    public CreationContext(
            @Nullable String creator,
            @Nullable Class<?> creatorType,
            @Nullable Method creatorMethod) {
        this(creator, shorten(creatorType), shorten(creatorMethod), null);
    }

    public CreationContext(
            @Nullable String creator,
            @Nullable String creatorType,
            @Nullable String creatorMethod,
            @Nullable Boolean scheduled) {
        this.creator = creator;
        this.creatorType = creatorType;
        this.creatorMethod = creatorMethod;
        this.scheduled = scheduled;
    }

    @Nullable
    public String getCreator() {
        return creator;
    }

    @Nullable
    public String getCreatorType() {
        return creatorType;
    }

    @Nullable
    public String getCreatorMethod() {
        return creatorMethod;
    }

    @Nullable
    public Boolean getScheduled() {
        return scheduled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CreationContext)) {
            return false;
        }
        CreationContext that = (CreationContext) o;
        return Objects.equals(creator, that.creator) &&
                Objects.equals(creatorType, that.creatorType) &&
                Objects.equals(creatorMethod, that.creatorMethod) &&
                Objects.equals(scheduled, that.scheduled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(creator, creatorType, creatorMethod, scheduled);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CreationContext.class.getSimpleName() + "{", "}")
                .add("creator='" + creator + "'")
                .add("creatorType='" + creatorType + "'")
                .add("creatorMethod='" + creatorMethod + "'")
                .add("scheduled=" + scheduled)
                .toString();
    }

    public boolean isEmpty() {
        return (creator == null || creator.isEmpty())
                && (creatorType == null || creatorType.isEmpty())
                && (creatorMethod == null || creatorMethod.isEmpty())
                && (scheduled == null);
    }
}
