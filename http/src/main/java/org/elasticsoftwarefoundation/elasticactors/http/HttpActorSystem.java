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

package org.elasticsoftwarefoundation.elasticactors.http;

import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.elasterix.elasticactors.*;
import org.elasticsoftwarefoundation.elasticactors.base.SpringBasedActorSystem;
import org.elasticsoftwarefoundation.elasticactors.base.serialization.JacksonActorRefDeserializer;
import org.elasticsoftwarefoundation.elasticactors.base.serialization.JacksonActorRefSerializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Joost van de Wijgerd
 */
public final class HttpActorSystem extends SpringBasedActorSystem {

    public HttpActorSystem() {
        super("http-beans.xml");
    }

    @Override
    protected void doInitialize(ApplicationContext applicationContext,ActorSystem actorSystem) {
        // hook up the actorService to the HttpServer
        HttpServer httpServer = applicationContext.getBean(HttpServer.class);
        httpServer.setActorSystem(actorSystem);
    }


    @Override
    public String getName() {
        return "Http";
    }

    @Override
    public int getNumberOfShards() {
        return 8;
    }

}
