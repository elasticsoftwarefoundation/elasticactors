/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors;

import java.net.InetAddress;

/**
 * @author Joost van de Wijgerd
 */
public class PhysicalNode {
    private final String id;
    private final InetAddress address;
    private final boolean local;

    public PhysicalNode(String id, InetAddress address, boolean local) {
        this.id = id;
        this.address = address;
        this.local = local;
    }

    public boolean isLocal() {
        return local;
    }

    public String getId() {
        return id;
    }

    public InetAddress getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PhysicalNode)) {
            return false;
        }
        PhysicalNode that = (PhysicalNode) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
