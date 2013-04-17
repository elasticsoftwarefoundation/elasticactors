package org.elasticsoftwarefoundation.elasticactors.http.actors;

import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorSystem;
import org.elasterix.elasticactors.TypedActor;
import org.elasticsoftwarefoundation.elasticactors.http.messages.HttpRequest;
import org.elasticsoftwarefoundation.elasticactors.http.messages.RegisterRouteMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import javax.annotation.PostConstruct;

/**
 * @author Joost van de Wijgerd
 */
@Lazy
public final class UserDispatcher extends TypedActor<HttpRequest> {
    private static final Logger logger = Logger.getLogger(UserDispatcher.class);

    private ActorSystem actorSystem;

    @Autowired
    public void setActorSystem(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    @PostConstruct
    public void init() {
        // register ourselves with the http server
        ActorRef httpServer = actorSystem.getParent().get("Http").serviceActorFor("httpServer");
        httpServer.tell(new RegisterRouteMessage("/users/*",actorSystem.serviceActorFor("userDispatcher")),actorSystem.serviceActorFor("userDispatcher"));
    }

    @Override
    public void onReceive(ActorRef sender, HttpRequest message) throws Exception {
        // inspect the message
        ActorRef userRef = actorSystem.actorFor(message.getUrl().substring(1));
        userRef.tell(message,sender);
    }
}
