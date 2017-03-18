package org.elasticsoftware.elasticactors.indexing.elasticsearch.indexer;

import com.google.common.base.Charsets;
import org.awaitility.Duration;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.indexing.elasticsearch.IndexConfig;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.ActorStateUpdate;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import static com.google.common.collect.Lists.newArrayList;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.FIVE_SECONDS;
import static org.awaitility.Duration.ONE_HUNDRED_MILLISECONDS;
import static org.elasticsoftware.elasticactors.indexing.elasticsearch.IndexConfig.VersioningStrategy.NONE;
import static org.elasticsoftware.elasticactors.indexing.elasticsearch.IndexConfig.VersioningStrategy.REINDEX_ON_ACTIVATE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class IndexerTest {

    private Node testNode;
    private String tmpElasticsearchDataDir = System.getProperty("java.io.tmpdir") + "/es-test-data/" + System.currentTimeMillis();
    private String tmpElasticsearchHomeDir = System.getProperty("java.io.tmpdir") + "/es-test-home/" + System.currentTimeMillis();

    private Client client;
    private Indexer indexer;

    @BeforeClass
    public void startElasticsearch() throws Exception {
        Settings.Builder settings = Settings.builder()
                .put("node.name", "test-node")
                .put("path.data", tmpElasticsearchDataDir)
                .put("path.home", tmpElasticsearchHomeDir)
                .put("cluster.name", "indexer-test-cluster")
                .put("transport.type", "local")
                .put("http.type", "netty3")
                .put("http.enabled", true);

        testNode = new PluginConfigurableNode(settings.build(), Collections.singletonList(Netty3Plugin.class));
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

    @Test
    public void testBasicVersionBasedReindexing() throws Exception {
        ActorStateUpdate update = createActorStateUpdateReindexing("1.0.0");
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
        assertTrue(client.admin().indices().prepareExists("test_index_v1-0").execute().get().isExists());
    }

    @Test
    public void testVersionBasedReindexingOldVersionDeleted() throws Exception {
        ActorStateUpdate update = createActorStateUpdateReindexing("1.0.0");
        when(update.getLifecycleStep()).thenReturn(ActorLifecycleStep.ACTIVATE);

        indexer.onUpdate(newArrayList(update));

        await()
                .atMost(FIVE_SECONDS)
                .pollInterval(ONE_HUNDRED_MILLISECONDS)
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

        when(update.getActorClass()).thenAnswer((Answer<Object>) invocation -> NoneVersioningMockActorClass.class);
        return update;
    }

    private ActorStateUpdate createActorStateUpdateReindexing(String version) {
        ActorStateUpdate update = createBasicActorStateUpdate();

        when(update.getActorClass()).thenAnswer((Answer<Object>) invocation -> ReindexingVersioningMockActorClass.class);
        when(update.getVersion()).thenReturn(version);
        return update;
    }

    private ActorStateUpdate createBasicActorStateUpdate() {
        ActorRef actorRef = mock(ActorRef.class);
        when(actorRef.getActorId()).thenReturn("1");

        ActorStateUpdate update = Mockito.mock(ActorStateUpdate.class);
        when(update.getActorRef()).thenReturn(actorRef);
        ByteBuffer charBuffer = ByteBuffer.wrap("{\"testField\": \"testData\"}".getBytes(Charsets.UTF_8));
        when(update.getSerializedState()).thenReturn(charBuffer);

        return update;
    }

    private static class PluginConfigurableNode extends Node {
        PluginConfigurableNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(InternalSettingsPreparer.prepareEnvironment(settings, null), classpathPlugins);
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
            new File(tmpElasticsearchDataDir).delete();
            new File(tmpElasticsearchHomeDir).delete();
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