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

package org.elasticsoftwarefoundation.elasticactors.http.messages;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * This will start a Server Sent Events stream to the client. The headers and response code will be sent to the
 * client but the {@link org.jboss.netty.channel.Channel} will not be closed. This needs to be followed up by a
 * {@link SseEvent} messages sent to the same actor
 *
 * @author Joost van de Wijgerd
 */
public class SseResponse extends HttpResponse {
    public SseResponse(@JsonProperty("statusCode") int statusCode,
                       @JsonProperty("headers") Map<String, List<String>> headers) {
        super(statusCode, headers, null);
    }
}
