/*
 * Copyright 2013 the original authors
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
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.http.HttpServer;
import org.elasticsoftware.elasticactors.http.messages.HttpRequest;
import org.elasticsoftware.elasticactors.http.messages.RegisterRouteMessage;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Joost van de Wijgerd
 */
@ServiceActor("httpServer")
public final class HttpService extends UntypedActor {
    private static final Logger logger = Logger.getLogger(HttpService.class);
    private final ConcurrentMap<String,ActorRef> routes = new ConcurrentHashMap<String,ActorRef>();
    private final PathMatcher pathMatcher = new AntPathMatcher();
    private final ActorSystem actorSystem;
    private final Configuration configuration;
    private HttpServer httpServer;

    @Inject
    public HttpService(ActorSystem actorSystem, Configuration configuration) {
        this.actorSystem = actorSystem;
        this.configuration = configuration;
    }

    @PostConstruct
    public void init() {
        ExecutorService bossExecutor = Executors.newCachedThreadPool(Executors.defaultThreadFactory());
        ExecutorService workerExecutor = Executors.newCachedThreadPool(Executors.defaultThreadFactory());
        int workers = Runtime.getRuntime().availableProcessors();
        NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(bossExecutor,workerExecutor,workers);
        httpServer = new HttpServer(channelFactory, this, actorSystem , 8080);
        httpServer.init();
    }

    @PreDestroy
    public void destroy() {
        httpServer.destroy();
    }

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
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
        ActorRef handlerRef = getHandler(request.getUrl());
        if(handlerRef != null) {
            logger.info(String.format("Found actor [%s]",handlerRef.toString()));
            handlerRef.tell(request,replyAddress);
            return true;
        } else {
            logger.info("No actor found to handle request");
            return false;
        }
    }

    private ActorRef getHandler(String urlPath) {
        // direct match?
        ActorRef handlerRef = routes.get(urlPath);
        if(handlerRef != null) {
            return handlerRef;
        }
        // Pattern match?
        List<String> matchingPatterns = new ArrayList<String>();
        for (String registeredPattern : this.routes.keySet()) {
            if (pathMatcher.match(registeredPattern, urlPath)) {
                matchingPatterns.add(registeredPattern);
            }
        }
        String bestPatternMatch = null;
        Comparator<String> patternComparator = pathMatcher.getPatternComparator(urlPath);
        if (!matchingPatterns.isEmpty()) {
            Collections.sort(matchingPatterns, patternComparator);
            if (logger.isDebugEnabled()) {
                logger.debug("Matching patterns for request [" + urlPath + "] are " + matchingPatterns);
            }
            bestPatternMatch = matchingPatterns.get(0);
        }
        if (bestPatternMatch != null) {
            return routes.get(bestPatternMatch);
        }
        return null;
    }
}
