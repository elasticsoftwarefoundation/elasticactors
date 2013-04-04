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

import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.UntypedActor;
import org.elasticsoftwarefoundation.elasticactors.http.messages.HttpRequest;
import org.elasticsoftwarefoundation.elasticactors.http.messages.RegisterRouteMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Joost van de Wijgerd
 */
public final class HttpService extends UntypedActor {
    private static final Logger logger = Logger.getLogger(HttpService.class);
    private final ConcurrentMap<String,ActorRef> routes = new ConcurrentHashMap<String,ActorRef>();

    @Override
    public void onReceive(Object message, ActorRef sender) throws Exception {
        if(message instanceof RegisterRouteMessage) {
            RegisterRouteMessage registerRouteMessage = (RegisterRouteMessage) message;
            routes.putIfAbsent(registerRouteMessage.getPattern(),registerRouteMessage.getHandlerRef());
            logger.info(String.format("Adding Route with pattern [%s] for Actor [%s]",registerRouteMessage.getPattern(),registerRouteMessage.getHandlerRef().toString()));
        } else {
            unhandled(message);
        }
    }

    public boolean doDispatch(HttpRequest request,ActorRef replyAddress) {
        logger.info(String.format("Dispatching Request [%s]",request.getUrl()));
        // match for routes, for now no fancy matching
        ActorRef handlerRef = routes.get(request.getUrl());
        if(handlerRef != null) {
            logger.info(String.format("Found actor [%s]",handlerRef.toString()));
            handlerRef.tell(request,replyAddress);
            return true;
        } else {
            logger.info("No actor found to handle request");
            return false;
        }
    }
}
