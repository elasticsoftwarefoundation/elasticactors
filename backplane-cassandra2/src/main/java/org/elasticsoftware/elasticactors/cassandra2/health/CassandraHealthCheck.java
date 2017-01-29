package org.elasticsoftware.elasticactors.cassandra2.health;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.elasticsoftware.elasticactors.health.HealthCheck;
import org.elasticsoftware.elasticactors.health.HealthCheckResult;
import org.springframework.beans.factory.annotation.Autowired;

import static org.elasticsoftware.elasticactors.health.HealthCheckResult.healthy;
import static org.elasticsoftware.elasticactors.health.HealthCheckResult.unhealthy;

/**
 * @author Rob de Boer
 */
public class CassandraHealthCheck implements HealthCheck {

    private final Session cassandraSession;

    @Autowired
    public CassandraHealthCheck(Session cassandraSession) {
        this.cassandraSession = cassandraSession;
    }

    public HealthCheckResult check() {
        if (cassandraSession.isClosed()) {
            return unhealthy("Cassandra Session appears to be closed");
        }

        try {
            String query = QueryBuilder.select().countAll().from("\"ElasticActors\"", "\"PersistentActors\"").getQueryString();
            ResultSet results = cassandraSession.execute(query);
            if (results.one() == null) {
                return unhealthy("No results found in Cassandra ElasticActors table");
            }
        } catch (Exception e) {
            return unhealthy("Unable to query Cassandra ElasticActors table: " + e.getMessage(), e);
        }

        return healthy();
    }

}
