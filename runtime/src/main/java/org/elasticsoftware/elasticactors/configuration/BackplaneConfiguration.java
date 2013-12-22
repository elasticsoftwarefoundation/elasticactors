package org.elasticsoftware.elasticactors.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

/**
 * @author Joost van de Wijgerd
 */
public class BackplaneConfiguration {
    @Autowired
    private Environment env;

    @PostConstruct
    public void initialize() {

    }
}
