/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.indexing.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsoftware.elasticactors.indexing.elasticsearch.indexer.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.aspectj.EnableSpringConfigured;
import org.springframework.core.env.Environment;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

@Configuration
@EnableSpringConfigured
public class IndexderAppConfig {

    private static final Logger logger = LoggerFactory.getLogger(IndexderAppConfig.class);

    @Autowired
    private Environment environment;

    private TransportClient client;

    @Bean(name = "indexingElasticsearchClient")
    public Client createElasticsearchClient() {
        String[] elasticsearchHosts = environment.getRequiredProperty("actor.indexing.elasticsearch.hosts", String[].class);
        Integer elasticsearchPort = environment.getProperty("actor.indexing.elasticsearch.port", Integer.class, 9300);
        String elasticsearchClusterName = environment.getProperty("actor.indexing.elasticsearch.cluster.name", String.class, "elasticsearch");

        logger.info("Creating elasticsearch client with hosts <{}>", Arrays.toString(elasticsearchHosts));

        Settings settings = Settings.builder()
                .put("cluster.name", elasticsearchClusterName).build();

        client = new PreBuiltTransportClient(settings);

        for (String elasticsearchHost : elasticsearchHosts) {
            try {
                client.addTransportAddress(new TransportAddress(InetAddress.getByName(elasticsearchHost), elasticsearchPort));
            } catch (UnknownHostException e) {
                throw new BeanCreationException("Could not add elasticsearch host <" + elasticsearchHost + "> to client configuration. Aborting", e);
            }
        }

        return client;
    }

    @Bean(name = "elasticsearchIndexer")
    public Indexer createIndexer(Client client) {
        return new Indexer(client);
    }

    @PreDestroy
    public void destroy() {
        if (client != null) {
            try {
                logger.info("Shutting down elasticsearch client");
                client.close();
            } catch (Exception e) {
                logger.warn("Exception while trying to shutdown the elasticsearch client", e);
            }
        }
    }
}
