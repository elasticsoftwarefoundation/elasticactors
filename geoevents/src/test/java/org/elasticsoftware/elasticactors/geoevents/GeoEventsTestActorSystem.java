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

package org.elasticsoftware.elasticactors.geoevents;

import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.DependsOn;
import org.elasticsoftware.elasticactors.base.SpringBasedActorSystem;
import org.springframework.context.ApplicationContext;

/**
 * @author Joost van de Wijgerd
 */
@DependsOn(dependencies = {"GeoEvents"})
public class GeoEventsTestActorSystem extends SpringBasedActorSystem {

    public GeoEventsTestActorSystem() {
        super("geoeventstest-beans.xml");
    }

    @Override
    protected void doInitialize(ApplicationContext applicationContext, ActorSystem actorSystem) {

    }

    @Override
    public String getName() {
        return "GeoEventsTest";
    }

    @Override
    public int getNumberOfShards() {
        return 8;
    }
}
