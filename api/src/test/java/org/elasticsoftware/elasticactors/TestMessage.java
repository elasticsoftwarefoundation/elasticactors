package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.Message;

/**
 * @author Joost van de Wijgerd
 */
@Message(serializationFramework = TestSerializationFramework.class)
public class TestMessage {
    private final String content;

    public TestMessage(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
