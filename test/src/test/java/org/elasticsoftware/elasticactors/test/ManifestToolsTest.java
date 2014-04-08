package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.base.state.StringState;
import org.elasticsoftware.elasticactors.util.ManifestTools;
import org.testng.annotations.Test;

/**
 * @author Joost van de Wijgerd
 */
public class ManifestToolsTest {
    @Test
    public void testGetVersion() {
        System.out.println(ManifestTools.extractVersion(StringState.class));
    }
}
