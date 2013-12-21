package org.elasticsoftware.elasticactors.http.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
public class ObjectMapperBuilderTest {
    @Test
    public void testBuildObjectMapper() {
        ActorRefFactory actorRefFactory = mock(ActorRefFactory.class);
        ObjectMapper objectMapper = new ObjectMapperBuilder(actorRefFactory,"1.0.0").build();
        // check if everything was scanned correctly
        assertNotNull(objectMapper);
    }
}
