package org.elasticsoftware.elasticactors.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
//import org.elasticsoftware.elasticactors.http.actors.HttpService;
import org.springframework.util.ResourceUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
public class DefaultConfigurationTest {
    @Test
    public void testLoadDefaultConfig() throws IOException {
        File configFile = ResourceUtils.getFile("classpath:ea-default.yaml");
        // get the yaml resource

        // yaml mapper
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        DefaultConfiguration configuration = objectMapper.readValue(new FileInputStream(configFile), DefaultConfiguration.class);

        assertEquals(configuration.getName(),"default");
        assertEquals(configuration.getNumberOfShards(),8);
        //assertEquals(configuration.getProperty(HttpService.class, "listenPort", Integer.class, 9090),new Integer(8080));
    }

    @Test
    public void testLoadIntoMap() throws IOException {
        File configFile = ResourceUtils.getFile("classpath:ea-default.yaml");
        // get the yaml resource

        // yaml mapper
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        Map configuration = objectMapper.readValue(new FileInputStream(configFile), Map.class);

        assertNotNull(configuration);
    }
}
