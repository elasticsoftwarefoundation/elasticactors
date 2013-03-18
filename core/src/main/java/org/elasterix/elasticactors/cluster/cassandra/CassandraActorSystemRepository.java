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

package org.elasterix.elasticactors.cluster.cassandra;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.*;
import org.elasterix.elasticactors.cluster.ActorSystemRepository;
import org.elasterix.elasticactors.cluster.RegisteredActorSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public class CassandraActorSystemRepository implements ActorSystemRepository {
    private ColumnFamilyTemplate<String,String> columnFamilyTemplate;
    private final SingleMapper singleResultMapper = new SingleMapper();
    private final ListMapper listResultMapper = new ListMapper();

    @Override
    public RegisteredActorSystem findByName(String name) {
        return columnFamilyTemplate.queryColumns(name,singleResultMapper);
    }

    @Override
    public List<RegisteredActorSystem> findAll() {
        //@todo: this is not correct, implement correctly
        IndexedSlicesPredicate allPredicate = new IndexedSlicesPredicate<String,String,String>(StringSerializer.get(),
                StringSerializer.get(),StringSerializer.get());
        MappedColumnFamilyResult<String,String,List<RegisteredActorSystem>> result = columnFamilyTemplate.queryColumns(allPredicate,listResultMapper);
        return result.getRow();
    }

    @Autowired
    public void setColumnFamilyTemplate(@Qualifier("actorSystemsColumnFamilyTemplate") ColumnFamilyTemplate<String, String> columnFamilyTemplate) {
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
                /*if(results.hasResults()) {
                    do {
                        resultList.add(new RegisteredActorSystem(results.getKey(),
                                                                 results.getInteger("nrOfShards"),
                                                                 results.getString("configurationClass")));
                        if(results.hasNext()) {
                            results = results.next();
                        } else {
                }*/
                return resultList;
            }
        }
}
