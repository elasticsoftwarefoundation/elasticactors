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

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public abstract class HttpMessage {
    private final Map<String,List<String>> headers;
    protected final byte[] content;

    protected HttpMessage(Map<String, List<String>> headers,  byte[] content) {
        this.headers = headers;
        this.content = content;
    }

    @JsonProperty("headers")
    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public final String getHeader(String name) {
        List<String> headerValues = headers.get(name);
        return headerValues == null || headerValues.isEmpty() ? null : headerValues.get(0);
    }

    public final List<String> getHeaders(String name) {
        List<String> headerValues = headers.get(name);
        return headerValues == null || headerValues.isEmpty() ? Collections.<String>emptyList() : headerValues;
    }

    @JsonIgnore
    public String getContentType() {
        return getHeader(HttpHeaders.Names.CONTENT_TYPE);
    }

    @JsonProperty("content")
    public byte[] getContent() {
        return content;
    }
}
