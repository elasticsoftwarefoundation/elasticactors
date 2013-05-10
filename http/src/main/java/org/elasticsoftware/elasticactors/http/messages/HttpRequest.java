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

package org.elasticsoftware.elasticactors.http.messages;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class HttpRequest extends HttpMessage {
    private final String url;
    private final String method;

    public HttpRequest(String url) {
        this("GET", url, Collections.<String,List<String>>emptyMap(),null);
    }

    public HttpRequest(String url,Map<String,List<String>> headers) {
        this("GET",url,headers,null);
    }

    @JsonCreator
    public HttpRequest(@JsonProperty("method") String method,
                       @JsonProperty("url") String url,
                       @JsonProperty("headers") Map<String,List<String>> headers,
                       @JsonProperty("content") byte[] content) {
        super(headers,content);
        this.url = url;
        this.method = method;
    }

    @JsonProperty("url")
    public String getUrl() {
        return url;
    }

    @JsonProperty("method")
    public String getMethod() {
        return method;
    }
}
