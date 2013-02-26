/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors;

/**
 *
 */
public final class VirtualNodeKey {
    private final String actorSystemName;
    private final int virtualNodeId;

    public VirtualNodeKey(String actorSystemName, int virtualNodeId) {
        this.actorSystemName = actorSystemName;
        this.virtualNodeId = virtualNodeId;
    }

    public int getVirtualNodeId() {
        return virtualNodeId;
    }

    public String getActorSystemName() {
        return actorSystemName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VirtualNodeKey that = (VirtualNodeKey) o;

        if (virtualNodeId != that.virtualNodeId) return false;
        if (!actorSystemName.equals(that.actorSystemName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = actorSystemName.hashCode();
        result = 31 * result + virtualNodeId;
        return result;
    }
}
