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

package org.elasticsoftware.elasticactors.cassandra.cluster;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyRowMapper;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.MappedColumnFamilyResult;
import me.prettyprint.hector.api.Keyspace;
import org.elasticsoftware.elasticactors.cluster.ActorSystemRepository;
import org.elasticsoftware.elasticactors.cluster.RegisteredActorSystem;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraActorSystemRepository implements ActorSystemRepository {
    private Keyspace keyspace;
    private ColumnFamilyTemplate<String,String> columnFamilyTemplate;
    private final SingleMapper singleResultMapper = new SingleMapper();
    private final ListMapper listResultMapper = new ListMapper();

    @Override
    public RegisteredActorSystem findByName(String name) {
        return columnFamilyTemplate.queryColumns(name,singleResultMapper);
    }

    @Override
    public List<RegisteredActorSystem> findAll() {
        KeyIterator<String> keyIterator = new KeyIterator<String>(keyspace,"ActorSystems",StringSerializer.get());
        MappedColumnFamilyResult<String,String,List<RegisteredActorSystem>> result = columnFamilyTemplate.queryColumns(keyIterator,listResultMapper);
        return result.getRow();
    }

    @Inject
    public void setKeyspace(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    @Inject
    public void setColumnFamilyTemplate(@Named("actorSystemsColumnFamilyTemplate") ColumnFamilyTemplate<String, String> columnFamilyTemplate) {
        this.columnFamilyTemplate = columnFamilyTemplate;
    }

    private static final class SingleMapper implements ColumnFamilyRowMapper<String,String,RegisteredActorSystem> {
        @Override
        public RegisteredActorSystem mapRow(ColumnFamilyResult<String, String> results) {
            if(results.hasResults()) {
                return new RegisteredActorSystem(results.getKey(),results.getInteger("nrOfShards"),results.getString("configurationClass"));
            } else {
                return null;
            }
        }
    }

    private static final class ListMapper implements ColumnFamilyRowMapper<String,String,List<RegisteredActorSystem>> {
            @Override
            public List<RegisteredActorSystem> mapRow(ColumnFamilyResult<String, String> results) {
                LinkedList<RegisteredActorSystem> resultList = new LinkedList<RegisteredActorSystem>();
                //@todo: implement this properly
                if(results.hasResults()) {
                   resultList.add(new RegisteredActorSystem(results.getKey(),
                                                            results.getInteger("nrOfShards"),
                                                            results.getString("configurationClass")));
                }
                if(results.hasNext()) {
                    do {
                        results = results.next();
                        resultList.add(new RegisteredActorSystem(results.getKey(),
                                                                 results.getInteger("nrOfShards"),
                                                                 results.getString("configurationClass")));
                    } while(results.hasNext());
                }

                return resultList;
            }
        }
}
