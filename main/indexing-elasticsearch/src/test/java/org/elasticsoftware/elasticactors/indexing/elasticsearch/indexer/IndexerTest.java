/*
 * Copyright 2013 - 2025 The Original Authors
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

import org.awaitility.Durations;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.indexing.elasticsearch.IndexConfig;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.ActorStateUpdate;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import static com.google.common.collect.Lists.newArrayList;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.ONE_HUNDRED_MILLISECONDS;
import static org.awaitility.Durations.ONE_SECOND;
import static org.elasticsoftware.elasticactors.indexing.elasticsearch.IndexConfig.VersioningStrategy.NONE;
import static org.elasticsoftware.elasticactors.indexing.elasticsearch.IndexConfig.VersioningStrategy.REINDEX_ON_ACTIVATE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class IndexerTest {

    private Node testNode;

    private final Path tmpElasticsearchHomeDir = Paths.get(System.getProperty("java.io.tmpdir"))
        .resolve("es-test-home")
        .resolve(String.format(
            "%d_%s_%s",
            System.currentTimeMillis(),
            getClass().getSimpleName(),
            UUIDTools.createRandomUUID()
        ))
        .toAbsolutePath();

    private final Path tmpElasticsearchDataDir = Paths.get(System.getProperty("java.io.tmpdir"))
        .resolve("es-test-data")
        .resolve(String.format(
            "%d_%s_%s",
            System.currentTimeMillis(),
            getClass().getSimpleName(),
            UUIDTools.createRandomUUID()
        ))
        .toAbsolutePath();

    private Client client;
    private Indexer indexer;

    @BeforeClass
    public void startElasticsearch() throws Exception {
        Settings.Builder settings = Settings.builder()
                .put("node.name", "test-node")
                .put("path.data", tmpElasticsearchDataDir)
                .put("path.home", tmpElasticsearchHomeDir)
                .put("cluster.name", "indexer-test-cluster")
                .put("transport.type", "netty4")
                .put("http.type", "netty4");

        testNode = new PluginConfigurableNode(settings.build(), Collections.singletonList(Netty4Plugin.class));
        testNode.start();

        client = testNode.client();
    }

    @BeforeMethod
    public void setup() {
        indexer = new Indexer(client);
    }

    @Test
    public void testBasicIndexingPostActivate() throws Exception {
        ActorStateUpdate update = createActorStateUpdateNoneVersioning();

        when(update.getLifecycleStep()).thenReturn(ActorLifecycleStep.ACTIVATE);

        indexer.onUpdate(newArrayList(update));

        await()
                .atMost(FIVE_SECONDS)
                .pollInterval(ONE_HUNDRED_MILLISECONDS)
                .until(() -> {
                    refreshIndices();
                    return client.prepareGet("test_index", "type_name", "1").execute().get().isExists();
                });

        // check index name is correctly created
        assertTrue(client.admin().indices().prepareExists("test_index_v1").execute().get().isExists());
    }

    @Test
    public void testBasicIndexingIncludedMessage() throws Exception {
        ActorStateUpdate update = createActorStateUpdateNoneVersioning();
        when(update.getMessageClass()).thenReturn(IncludedMessageClass.class);

        indexer.onUpdate(newArrayList(update));

        await()
                .atMost(FIVE_SECONDS)
                .pollInterval(ONE_HUNDRED_MILLISECONDS)
                .until(() -> {
                    refreshIndices();
                    return client.prepareGet("test_index_v1", "type_name", "1").execute().get().isExists();
                });
    }

    @Test
    public void testNoIndexingExcludedMessage() throws Exception {
        ActorStateUpdate update = createActorStateUpdateNoneVersioning();
        when(update.getMessageClass()).thenReturn(ExcludedMessageClass.class);

        indexer.onUpdate(newArrayList(update));

        // check indices are not created, thus no indexing is has been performed
        assertFalse(client.admin().indices().prepareExists("test_index_v1").execute().get().isExists());
        assertFalse(client.admin().indices().prepareExists("test_index").execute().get().isExists());
    }

    @Test
    public void testDeleteOnDestroy() throws Exception {
        // make sure document is indexed
        testBasicIndexingPostActivate();

        ActorStateUpdate update = createActorStateUpdateNoneVersioning();
        when(update.getLifecycleStep()).thenReturn(ActorLifecycleStep.DESTROY);

        indexer.onUpdate(newArrayList(update));

        await()
                .atMost(FIVE_SECONDS)
                .pollInterval(ONE_HUNDRED_MILLISECONDS)
                .until(() -> {
                    refreshIndices();
                    return !client.prepareGet("test_index_v1", "type_name", "1").execute().get().isExists();
                });
    }

    @Test(enabled = false)
    public void testBasicVersionBasedReindexing() throws Exception {
        ActorStateUpdate update = createActorStateUpdateReindexing("1.0.0");
        when(update.getLifecycleStep()).thenReturn(ActorLifecycleStep.ACTIVATE);

        indexer.onUpdate(newArrayList(update));

        await()
                .atMost(Durations.ONE_MINUTE)
                .pollInterval(ONE_HUNDRED_MILLISECONDS)
                .until(() -> {
                    refreshIndices();
                    return client.prepareGet("test_index", "type_name", "1").execute().get().isExists();
                });

        // check index name is correctly created
        assertTrue(client.admin().indices().prepareExists("test_index_v1-0").execute().get().isExists());
    }

    @Test(enabled = false)
    public void testVersionBasedReindexingOldVersionDeleted() throws Exception {
        ActorStateUpdate update = createActorStateUpdateReindexing("1.0.0");
        when(update.getLifecycleStep()).thenReturn(ActorLifecycleStep.ACTIVATE);

        indexer.onUpdate(newArrayList(update));

        await()
                .atMost(Durations.ONE_MINUTE)
                .pollInterval(ONE_SECOND)
                .until(() -> {
                    refreshIndices();
                    return client.prepareGet("test_index_v1-0", "type_name", "1").execute().get().isExists();
                });

        indexer.getActivatedActors().clear();

        update = createActorStateUpdateReindexing("1.1.0");
        when(update.getLifecycleStep()).thenReturn(ActorLifecycleStep.ACTIVATE);

        indexer.onUpdate(newArrayList(update));

        await()
                .atMost(FIVE_SECONDS)
                .pollInterval(ONE_HUNDRED_MILLISECONDS)
                .until(() -> {
                    refreshIndices();
                    return !client.prepareGet("test_index_v1-0", "type_name", "1").execute().get().isExists()
                            && client.prepareGet("test_index_v1-1", "type_name", "1").execute().get().isExists();
                });
    }

    private ActorStateUpdate createActorStateUpdateNoneVersioning() {
        ActorStateUpdate update = createBasicActorStateUpdate();

        when(update.getActorClass()).thenAnswer(invocation -> NoneVersioningMockActorClass.class);
        return update;
    }

    private ActorStateUpdate createActorStateUpdateReindexing(String version) {
        ActorStateUpdate update = createBasicActorStateUpdate();

        when(update.getActorClass()).thenAnswer(invocation -> ReindexingVersioningMockActorClass.class);
        when(update.getVersion()).thenReturn(version);
        return update;
    }

    private ActorStateUpdate createBasicActorStateUpdate() {
        ActorRef actorRef = mock(ActorRef.class);
        when(actorRef.getActorId()).thenReturn("1");

        ActorStateUpdate update = Mockito.mock(ActorStateUpdate.class);
        when(update.getActorRef()).thenReturn(actorRef);
        ByteBuffer charBuffer = ByteBuffer.wrap("{\"testField\": \"testData\"}".getBytes(StandardCharsets.UTF_8));
        when(update.getSerializedState()).thenReturn(charBuffer);

        return update;
    }

    private static class PluginConfigurableNode extends Node {
        PluginConfigurableNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(InternalSettingsPreparer.prepareEnvironment(settings, new HashMap<>(), null, null), classpathPlugins, false);
        }
    }

    @IndexConfig(includedMessages = {IncludedMessageClass.class}, indexName = "test_index", typeName = "type_name", versioningStrategy = NONE)
    private abstract static class NoneVersioningMockActorClass implements ElasticActor {

    }

    @IndexConfig(includedMessages = {IncludedMessageClass.class}, indexName = "test_index", typeName = "type_name", versioningStrategy = REINDEX_ON_ACTIVATE)
    private abstract static class ReindexingVersioningMockActorClass implements ElasticActor {

    }

    private static class IncludedMessageClass {

    }

    private static class ExcludedMessageClass {

    }

    @AfterClass
    public void shutdownElasticsearch() throws Exception {
        try {
            if (client != null) {
                client.close();
            }

            if (testNode != null) {
                testNode.close();
            }
        } finally {
            recursiveDelete(tmpElasticsearchDataDir);
            recursiveDelete(tmpElasticsearchHomeDir);
        }
    }

    private void recursiveDelete(Path directory) throws IOException {
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(
                        Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return super.visitFile(file, attrs);
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                        throws IOException {
                    Files.delete(dir);
                    return super.postVisitDirectory(dir, exc);
                }
            });
        } catch (NoSuchFileException e) {
            if (!directory.equals(tmpElasticsearchHomeDir)) {
                throw e;
            }
        }
    }

    @AfterMethod
    public void tearDown() {
        client.admin().indices().prepareDelete("_all").get();
    }

    private void refreshIndices() {
        client.admin().indices().prepareRefresh("_all").get();
    }
}