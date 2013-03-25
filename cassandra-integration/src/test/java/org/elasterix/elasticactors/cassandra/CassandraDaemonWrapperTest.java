/*
 * Copyright 2013 eBuddy BV
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

package org.elasterix.elasticactors.cassandra;

import org.apache.cassandra.service.CassandraDaemon;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
public class CassandraDaemonWrapperTest {
    @Test
    public void testJSVCLifecycle() throws IOException {
        CassandraDaemonWrapper cassandraDaemonWrapper = new CassandraDaemonWrapper();
        CassandraDaemon cassandraDaemon = cassandraDaemonWrapper.getCassandraDaemonInstance();
        assertNotNull(cassandraDaemon);
    }
}
