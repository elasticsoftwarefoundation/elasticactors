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

package org.elasticsoftwarefoundation.elasticactors.http.actors;

import com.google.common.base.Charsets;
import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.TypedActor;
import org.elasticsoftwarefoundation.elasticactors.http.messages.HttpRequest;
import org.elasticsoftwarefoundation.elasticactors.http.messages.HttpResponse;
import org.elasticsoftwarefoundation.elasticactors.http.messages.RegisterRouteMessage;

/**
 * @author Joost van de Wijgerd
 */
public final class User extends TypedActor<HttpRequest> {
    private static final Logger logger = Logger.getLogger(User.class);

    @Override
    public void postActivate(String previousVersion) throws Exception {
        // register ourselves with the http server
        ActorRef httpServer = getSystem().getParent().get("Http").serviceActorFor("httpServer");
        httpServer.tell(new RegisterRouteMessage(String.format("/%s", getSelf().getActorId()),getSelf()),getSelf());
    }

    @Override
    public void onReceive(HttpRequest message, ActorRef sender) throws Exception {
        logger.info("Got request");
        // send something back
        sender.tell(new HttpResponse(200,"text/plain","HelloWorld".getBytes(Charsets.UTF_8)),getSelf());
    }
}
