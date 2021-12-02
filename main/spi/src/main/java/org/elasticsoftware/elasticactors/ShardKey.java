/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public final class ShardKey implements Comparable<ShardKey> {

    private static final String SEPARATOR = "/shards/";

    private final String actorSystemName;
    private final int shardId;
    private final String spec;

    public ShardKey(String actorSystemName, int shardId) {
        this.actorSystemName = actorSystemName;
        this.shardId = shardId;
        this.spec = actorSystemName + SEPARATOR + shardId;
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
        int index = shardKey.lastIndexOf(SEPARATOR);
        if(index < 0) {
            throw new IllegalArgumentException("Missing /shards/ index");
        }
        return new ShardKey(
            shardKey.substring(0, index),
            Integer.parseInt(shardKey.substring(index + SEPARATOR.length()))
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShardKey)) {
            return false;
        }

        ShardKey that = (ShardKey) o;

        return (shardId == that.shardId)
            && (actorSystemName.equals(that.actorSystemName));
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
