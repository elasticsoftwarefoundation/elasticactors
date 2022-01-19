/*
 *   Copyright 2013 - 2022 The Original Authors
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
public final class NodeKey {
    private final String actorSystemName;
    private final String nodeId;

    public NodeKey(String actorSystemName, String nodeId) {
        this.actorSystemName = actorSystemName;
        this.nodeId = nodeId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getActorSystemName() {
        return actorSystemName;
    }

    @Override
    public String toString() {
        return actorSystemName + "/nodes/" + nodeId;
    }

    public static NodeKey fromString(String shardKey) {
        int separator = shardKey.lastIndexOf("/nodes/");
        if(separator < 0) {
            throw new IllegalArgumentException("Missing /nodes/ separator");
        }
        String[] components = shardKey.split("/");
        return new NodeKey(components[0],components[2]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NodeKey)) {
            return false;
        }

        NodeKey that = (NodeKey) o;

        return nodeId.equals(that.nodeId)
            && actorSystemName.equals(that.actorSystemName);
    }

    @Override
    public int hashCode() {
        int result = actorSystemName.hashCode();
        result = 31 * result + nodeId.hashCode();
        return result;
    }
}
