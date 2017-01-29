/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public final class ShardKey implements Comparable<ShardKey> {
    private final String actorSystemName;
    private final int shardId;
    private final String spec;

    public ShardKey(String actorSystemName, int shardId) {
        this.actorSystemName = actorSystemName;
        this.shardId = shardId;
        this.spec = String.format("%s/shards/%d",actorSystemName,shardId);
    }

    public int getShardId() {
        return shardId;
    }

    public String getActorSystemName() {
        return actorSystemName;
    }

    @Override
    public String toString() {
        return this.spec;
    }

    public static ShardKey fromString(String shardKey) {
        int separator = shardKey.lastIndexOf("/shards/");
        if(separator < 0) {
            throw new IllegalArgumentException("Missing /shards/ separator");
        }
        String[] components = shardKey.split("/");
        return new ShardKey(components[0],Integer.parseInt(components[2]));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardKey that = (ShardKey) o;

        if (shardId != that.shardId) return false;
        if (!actorSystemName.equals(that.actorSystemName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = actorSystemName.hashCode();
        result = 768 * result + shardId;
        return result;
    }

    @Override
    public int compareTo(ShardKey other) {
        int result = this.actorSystemName.compareTo(other.actorSystemName);
        return (result != 0) ? result : Integer.compare(this.shardId, other.shardId);
    }
}
