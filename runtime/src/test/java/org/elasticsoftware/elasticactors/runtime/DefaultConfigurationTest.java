/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.springframework.util.ResourceUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import static org.testng.Assert.*;

//import org.elasticsoftware.elasticactors.http.actors.HttpService;

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
        assertNotNull(configuration.getRemoteConfigurations());
        assertFalse(configuration.getRemoteConfigurations().isEmpty());
        assertEquals(configuration.getRemoteConfigurations().size(),2);
        assertEquals(configuration.getRemoteConfigurations().get(0).getClusterName(),"test2.elasticsoftware.org");
        assertEquals(configuration.getRemoteConfigurations().get(1).getClusterName(),"test3.elasticsoftware.org");
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
