/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.indexing.elasticsearch.indexer;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsoftware.elasticactors.indexing.elasticsearch.IndexConfig;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.ActorStateUpdate;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateListener;
import org.elasticsoftware.elasticactors.util.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsoftware.elasticactors.indexing.elasticsearch.IndexConfig.VersioningStrategy.NONE;
import static org.elasticsoftware.elasticactors.indexing.elasticsearch.IndexConfig.VersioningStrategy.REINDEX_ON_ACTIVATE;

public final class Indexer implements ActorStateUpdateListener {

    private static final Logger logger = LoggerFactory.getLogger(Indexer.class);

    private Client client;

    private final Map<String, List<String>> oldVersionOfIndices = newHashMap();
    private final Set<String> activatedActors = newHashSet();
    private final Set<String> setupIndices = newHashSet();

    @Autowired
    public Indexer(Client client) {
        this.client = client;
    }

    @Override
    public void onUpdate(List<? extends ActorStateUpdate> updates) {
        updates.stream().filter(
                update -> update.getActorClass().getAnnotation(IndexConfig.class) != null
        ).forEach(update ->
                {
                    IndexConfig indexConfig = update.getActorClass().getAnnotation(IndexConfig.class);

                    Class messageClass = update.getMessageClass();
                    ActorLifecycleStep lifecycleStep = update.getLifecycleStep();

                    if (messageClass != null && ArrayUtils.contains(indexConfig.includedMessages(), messageClass)) {

                        indexActorState(indexConfig, update);

                    } else if (lifecycleStep != null) {
                        if (lifecycleStep == ActorLifecycleStep.DESTROY) {

                            deleteActorState(indexConfig, update);

                        } else if (Arrays.binarySearch(indexConfig.indexOn(), lifecycleStep) >= 0) {

                            indexActorState(indexConfig, update);

                        }

                        if (lifecycleStep == ActorLifecycleStep.ACTIVATE && indexConfig.versioningStrategy() == REINDEX_ON_ACTIVATE) {
                            if (!activatedActors.contains(update.getActorRef().getActorId())) {
                                deleteOldVersionsOfActor(indexConfig, update);
                                activatedActors.add(update.getActorRef().getActorId());
                            }
                        }
                    }
                }
        );
    }

    private void deleteActorState(IndexConfig indexConfig, ActorStateUpdate update) {
        doDeleteActorState(constructIndexName(indexConfig, update), indexConfig.typeName(), update.getActorRef().getActorId());
    }

    private void deleteOldVersionsOfActor(IndexConfig indexConfig, ActorStateUpdate update) {
        List<String> previousIndices = findPreviousIndices(indexConfig, update);

        for (String indexName : previousIndices) {
            doDeleteActorState(indexName, indexConfig.typeName(), update.getActorRef().getActorId());
        }
    }

    private void doDeleteActorState(String indexName, String typeName, String actorId) {
        client.prepareDelete(indexName, typeName, actorId)
                .execute(new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse deleteResponse) {
                        logger.debug("Successfully deleted actor {{}} from elasticsearch", actorId);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Failed to delete actor {{}} from elasticsearch", actorId, e);
                    }
                });
    }

    private void indexActorState(IndexConfig indexConfig, ActorStateUpdate update) {
        String indexName = constructIndexName(indexConfig, update);

        if (!setupIndices.contains(indexName)) {
            try {
                setUpIndexAliases(indexConfig, update);
                setupIndices.add(indexName);
            } catch (Exception e) {
                logger.error("Error while trying to setup aliases for index {{}}. Actor <{}> will not be indexed in elasticsearch",
                        indexName, update.getActorRef(), e);
                return;
            }
        }

        try (ByteBufferBackedInputStream is = new ByteBufferBackedInputStream(update.getSerializedState())) {
            String actorState = CharStreams.toString(new InputStreamReader(is, StandardCharsets.UTF_8));

            client.prepareIndex(indexName, indexConfig.typeName(), update.getActorRef().getActorId())
                    .setOpType(IndexRequest.OpType.INDEX)
                    .setSource(update.getActorRef().getActorId(), actorState)
                    .execute(new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            logger.debug("Successfully indexed actor {{}} in elasticsearch", update.getActorRef());
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("Failed to index actor {{}} in elasticsearch", update.getActorRef(), e);
                        }
                    });
        } catch (Exception e) {
            logger.error("Encountered error while trying to index actor {{}} in elasticsearch", update.getActorRef(), e);
        }
    }

    private String constructIndexName(IndexConfig indexConfig, ActorStateUpdate update) {
        return indexConfig.indexName() + "_" + constructCurrentIndexVersion(indexConfig, update);
    }

    private String constructCurrentIndexVersion(IndexConfig indexConfig, ActorStateUpdate update) {
        if (indexConfig.versioningStrategy().equals(NONE)) {
            return "v1";
        } else {
            // the assumption is that the version will be in a format of X.Y.Z
            // where X is major version, Y is minor version and Z is bug fix
            // for now, only store the version as "vX-Y".
            // if version is in another format, just use that
            String[] versions = update.getVersion().split("\\.");

            if (versions.length > 1) {
                return "v" + versions[0] + "-" + versions[1];
            } else {
                return "v" + update.getVersion();
            }
        }
    }

    private void setUpIndexAliases(IndexConfig indexConfig, ActorStateUpdate update) throws Exception {
        String baseIndexName = indexConfig.indexName();
        String fullIndexName = constructIndexName(indexConfig, update);

        IndicesExistsResponse indicesExistsResponse = client.admin()
                .indices()
                .prepareExists(fullIndexName)
                .execute().get();

        if (!indicesExistsResponse.isExists()) {
            client.admin()
                    .indices()
                    .prepareCreate(fullIndexName)
                    .execute().get();

            client.admin()
                    .indices()
                    .prepareAliases()
                    .addAlias(fullIndexName, baseIndexName)
                    .execute().get();
        }
    }

    private List<String> findPreviousIndices(IndexConfig indexConfig, ActorStateUpdate update) {
        if (oldVersionOfIndices.containsKey(indexConfig.indexName())) {
            return oldVersionOfIndices.get(indexConfig.indexName());
        }

        List<String> previousIndices = newArrayList();

        try {
            String baseIndexName = indexConfig.indexName();
            String fullIndexName = constructIndexName(indexConfig, update);

            GetIndexResponse indexResponse = client.admin().indices()
                    .prepareGetIndex()
                    .addIndices(baseIndexName + "*")
                    .execute().get();

            for (String index : indexResponse.indices()) {
                if (!index.equals(baseIndexName) && index.equals(fullIndexName)) {
                    previousIndices.add(index);
                }
            }
        } catch (Exception e) {
            logger.error("Encountered error while trying to find previous version of indices for base index name {" +
                    indexConfig.indexName() +
                    "} Old versions of actors indexed here won't be removed", e);
        }

        oldVersionOfIndices.put(indexConfig.indexName(), previousIndices);

        return previousIndices;
    }

    @VisibleForTesting
    Set<String> getActivatedActors() {
        return activatedActors;
    }
}
