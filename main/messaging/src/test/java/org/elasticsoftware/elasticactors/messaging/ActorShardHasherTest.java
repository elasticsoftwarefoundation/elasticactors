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

package org.elasticsoftware.elasticactors.messaging;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ActorShardHasherTest {

    @Test
    public void testNegativeValue() {
        ActorShardHasher hasher = new ActorShardHasher(30803);

        Assert.assertTrue(hasher.hashStringToInt("accounts/341519da-c84f-4762-8091-63efd087656c") >= 0);
    }
}
