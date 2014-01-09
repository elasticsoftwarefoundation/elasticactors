package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ShardedScheduledWorkManager;
import org.elasticsoftware.elasticactors.util.concurrent.WorkExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.WorkExecutorFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ShardedScheduler implements SchedulerService,WorkExecutorFactory {
    private ShardedScheduledWorkManager<ShardKey,ScheduledMessage> workManager;
    private ScheduledMessageRepository scheduledMessageRepository;
    private InternalActorSystem actorSystem;


    @PostConstruct
    public void init() {
        ExecutorService executorService = Executors.newCachedThreadPool(new DaemonThreadFactory("SCHEDULER"));
        workManager = new ShardedScheduledWorkManager<ShardKey, ScheduledMessage>(executorService,this,Runtime.getRuntime().availableProcessors());
        workManager.init();
    }

    @PreDestroy
    public void destroy() {
        workManager.destroy();
    }

    @Inject
    public void setScheduledMessageRepository(ScheduledMessageRepository scheduledMessageRepository) {
        this.scheduledMessageRepository = scheduledMessageRepository;
    }

    @Inject
    public void setActorSystem(InternalActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    @Override
    public void registerShard(ShardKey shardKey) {
        // obtain the scheduler shard
        workManager.registerShard(shardKey);
        // fetch block from repository
        // @todo: for now we'll fetch all, this obviously has memory issues
        List<ScheduledMessage> scheduledMessages = scheduledMessageRepository.getAll(shardKey);
        workManager.schedule(shardKey,scheduledMessages.toArray(new ScheduledMessage[scheduledMessages.size()]));
    }

    @Override
    public void unregisterShard(ShardKey shardKey) {
        workManager.unregisterShard(shardKey);
    }

    @Override
    public void scheduleOnce(ActorRef sender, Object message, ActorRef receiver, long delay, TimeUnit timeUnit) {
        // this method only works when sender is a local persistent actor (so no temp or service actor)
        if(sender instanceof ActorContainerRef) {
            ActorContainer actorContainer = ((ActorContainerRef)sender).get();
            if(actorContainer instanceof ActorShard) {
                ActorShard actorShard = (ActorShard) actorContainer;
                if(actorShard.getOwningNode().isLocal()) {
                    // we're in business
                    try {
                        long fireTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(delay,timeUnit);
                        MessageSerializer serializer = actorSystem.getSerializer(message.getClass());
                        ScheduledMessage scheduledMessage = new ScheduledMessageImpl(fireTime,sender,receiver,message.getClass(),serializer.serialize(message));
                        scheduledMessageRepository.create(actorShard.getKey(), scheduledMessage);
                        workManager.schedule(actorShard.getKey(),scheduledMessage);
                        return;
                    } catch(Exception e) {
                        throw new RejectedExecutionException(e);
                    }
                }
            }
        }
        // sender param didn't fit the criteria
        throw new IllegalArgumentException(format("sender ref: %s needs to be a non-temp, non-service, locally sharded actor ref",sender.toString()));
    }

    @Override
    public WorkExecutor create() {
        return new ScheduledMessageExecutor();
    }

    private final class ScheduledMessageExecutor implements WorkExecutor<ShardKey,ScheduledMessage> {

        @Override
        public void execute(final ShardKey shardKey,final ScheduledMessage message) {
            // send the message
            final ActorRef receiverRef = message.getReceiver();
            receiverRef.tell(message.getMessageBytes(),message.getSender());
            // remove from the backing store
            scheduledMessageRepository.delete(shardKey, message);
        }
    }
}
