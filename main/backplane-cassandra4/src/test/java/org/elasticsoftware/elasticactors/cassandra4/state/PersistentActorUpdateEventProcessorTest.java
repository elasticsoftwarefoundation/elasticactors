/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.cassandra4.state;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import org.elasticsoftware.elasticactors.cassandra.common.state.PersistentActorUpdateEvent;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class PersistentActorUpdateEventProcessorTest {

    @Mock
    private CqlSession cqlSession;

    @Mock
    private PreparedStatement insertStatement;

    @Mock
    private PreparedStatement deleteStatement;

    @Mock
    private PersistentActorUpdateEvent event;

    @Mock
    private BatchStatement batchStatement;

    @Mock
    private Node cassandraNode;

    @Mock
    private EndPoint cassandraEndPoint;

    @Mock
    private DriverContext driverContext;

    private PersistentActorUpdateEventProcessor processor;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(cqlSession.prepare(PersistentActorUpdateEventProcessor.INSERT_QUERY)).thenReturn(insertStatement);
        when(cqlSession.prepare(PersistentActorUpdateEventProcessor.DELETE_QUERY)).thenReturn(deleteStatement);


        when(cassandraNode.getEndPoint()).thenReturn(cassandraEndPoint);
        when(cassandraEndPoint.resolve()).thenReturn(InetSocketAddress.createUnresolved("localhost", 9042));

        when(batchStatement.computeSizeInBytes(any(DriverContext.class))).thenReturn(65000);
        when(cqlSession.getContext()).thenReturn(driverContext);
        when(driverContext.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
        when(driverContext.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);


        processor = new PersistentActorUpdateEventProcessor(cqlSession);
    }

    @Test
    public void testProcessSingleEvent() {
        when(event.hasPersistentActorBytes()).thenReturn(true);
        when(event.rowKey()).thenReturn(new String[]{"key1", "key2"});
        when(event.persistentActorId()).thenReturn("actorId");
        when(event.persistentActorBytes()).thenReturn(ByteBuffer.wrap(new byte[]{1, 2, 3}));

        BoundStatement boundStatement = mock(BoundStatement.class);
        when(insertStatement.bind(any(), any(), any(), any())).thenReturn(boundStatement);

        processor.process(event);

        verify(insertStatement).bind(eq("key1"), eq("key2"), eq("actorId"), any(ByteBuffer.class));
        verify(cqlSession).execute(any(BoundStatement.class));
    }

    @Test
    public void testProcessMultipleEvents() {
        when(event.hasPersistentActorBytes()).thenReturn(true);
        when(event.rowKey()).thenReturn(new String[]{"key1", "key2"});
        when(event.persistentActorId()).thenReturn("actorId");
        when(event.persistentActorBytes()).thenReturn(ByteBuffer.wrap(new byte[]{1, 2, 3}));

        BoundStatement boundStatement1 = mock(BoundStatement.class);
        BoundStatement boundStatement2 = mock(BoundStatement.class);
        when(insertStatement.bind(any(), any(), any(), any())).thenReturn(boundStatement1).thenReturn(boundStatement2);

        processor.process(List.of(event, event));

        verify(insertStatement, times(2)).bind(eq("key1"), eq("key2"), eq("actorId"), any(ByteBuffer.class));
        verify(cqlSession).execute(any(BatchStatement.class));
    }

    @Test
    public void testProcessEmptyEvents() {
        processor.process(Collections.emptyList());

        verifyNoInteractions(insertStatement);
        verifyNoInteractions(deleteStatement);
    }

    @Test
    public void testProcessBatchTooLargeWithTwoEvents() {
        when(event.hasPersistentActorBytes()).thenReturn(true);
        when(event.rowKey()).thenReturn(new String[]{"key1", "key2"});
        when(event.persistentActorId()).thenReturn("actorId");
        when(event.persistentActorBytes()).thenReturn(ByteBuffer.wrap(new byte[1024]));

        BoundStatement boundStatement1 = mock(BoundStatement.class);
        BoundStatement boundStatement2 = mock(BoundStatement.class);
        when(insertStatement.bind(any(), any(), any(), any())).thenReturn(boundStatement1).thenReturn(boundStatement2);

        doThrow(new InvalidQueryException(cassandraNode, "Batch too large"))
                .when(cqlSession)
                .execute(any(BatchStatement.class));

        processor.process(List.of(event, event));

        verify(cqlSession, times(1)).execute(any(BatchStatement.class));
        verify(insertStatement, times(4)).bind("key1", "key2", "actorId", ByteBuffer.wrap(new byte[1024]));
        verify(cqlSession, times(2)).execute(any(BoundStatement.class));

    }

    @Test
    public void testProcessBatchTooLargeWithThreeEvents() {
        when(event.hasPersistentActorBytes()).thenReturn(true);
        when(event.rowKey()).thenReturn(new String[]{"key1", "key2"});
        when(event.persistentActorId()).thenReturn("actorId");
        when(event.persistentActorBytes()).thenReturn(ByteBuffer.wrap(new byte[1024]));

        BoundStatement boundStatement1 = mock(BoundStatement.class);
        BoundStatement boundStatement2 = mock(BoundStatement.class);
        when(insertStatement.bind(any(), any(), any(), any())).thenReturn(boundStatement1).thenReturn(boundStatement2);

        when(cqlSession.execute(any(BatchStatement.class)))
                .thenThrow(new InvalidQueryException(cassandraNode, "Batch too large"))
                .thenReturn(null);

        processor.process(List.of(event, event, event));

        verify(cqlSession, times(2)).execute(any(BatchStatement.class));
        verify(insertStatement, times(6)).bind("key1", "key2", "actorId", ByteBuffer.wrap(new byte[1024]));
        verify(cqlSession, times(1)).execute(any(BoundStatement.class));
    }

    @Test
    public void testProcessBatchTooLargeWithNineEvents() {
        when(event.hasPersistentActorBytes()).thenReturn(true);
        when(event.rowKey()).thenReturn(new String[]{"key1", "key2"});
        when(event.persistentActorId()).thenReturn("actorId");
        when(event.persistentActorBytes()).thenReturn(ByteBuffer.wrap(new byte[1024]));

        BoundStatement boundStatement1 = mock(BoundStatement.class);
        BoundStatement boundStatement2 = mock(BoundStatement.class);
        when(insertStatement.bind(any(), any(), any(), any())).thenReturn(boundStatement1).thenReturn(boundStatement2);

        when(cqlSession.execute(any(BatchStatement.class)))
                .thenThrow(new InvalidQueryException(cassandraNode, "Batch too large"))
                .thenReturn(null)
                .thenReturn(null);

        processor.process(List.of(event, event, event, event, event, event, event, event, event));

        verify(cqlSession, times(3)).execute(any(BatchStatement.class));
        verify(insertStatement, times(18)).bind("key1", "key2", "actorId", ByteBuffer.wrap(new byte[1024]));
    }

}

