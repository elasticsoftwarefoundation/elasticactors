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

package org.elasterix.elasticactors.examples.httpclient.actors;

import com.google.common.base.Charsets;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.TypedActor;
import org.elasterix.elasticactors.examples.httpclient.messages.HttpResponse;

/**
 * @author Joost van de Wijgerd
 */
public class HttpResponseListener extends TypedActor<HttpResponse> {
    @Override
    public void onReceive(ActorRef sender, HttpResponse message) throws Exception {
        System.out.println(String.format("Got response with status [%d] and content type [%s]",message.getStatusCode(),message.getContentType()));
        System.out.println(new String(message.getResponseBody(), Charsets.UTF_8));
    }
}
