/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.http.actors;

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.http.messages.HttpRequest;
import org.elasticsoftware.elasticactors.http.messages.RegisterRouteMessage;

/**
 * @author Joost van de Wijgerd
 */
@ServiceActor("userDispatcher")
public final class UserDispatcher extends TypedActor<HttpRequest> {
    private static final Logger logger = Logger.getLogger(UserDispatcher.class);

    @Override
    public void postActivate(String previousVersion) throws Exception {
        // register ourselves with the http server
        ActorRef httpServer = getSystem().getParent().get("Http").serviceActorFor("httpServer");
        httpServer.tell(new RegisterRouteMessage("/users/*",getSelf()),getSelf());
    }

    @Override
    public void onReceive(ActorRef sender, HttpRequest message) throws Exception {
        // inspect the message
        ActorRef userRef = getSystem().actorFor(message.getUrl().substring(1));
        userRef.tell(message,sender);
    }
}
