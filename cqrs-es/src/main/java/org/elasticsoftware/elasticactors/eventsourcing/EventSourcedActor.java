package org.elasticsoftware.elasticactors.eventsourcing;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cqrs.*;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.PersistenceAdvisor;

import java.lang.reflect.Constructor;
import java.util.HashMap;

public abstract class EventSourcedActor extends TypedActor<Object> implements PersistenceAdvisor {
    private final CommandContextHolder commandContextHolder = new CommandContextHolder();
    private final HashMap<Class<? extends Command>, CommandHandler> commandHandlers = new HashMap<>();
    private final HashMap<Class<? extends Query>, QueryHandler> queryHandlers = new HashMap<>();
    private final HashMap<Class<? extends SourcedEvent>, SourcedEventHandler> eventHandlers = new HashMap<>();
    private final Class<? extends ActorState> stateClass = resolveActorStateClass();
    private final Constructor<? extends ReadOnlyState> stateAdapterConstructor;

    protected EventSourcedActor(Class<? extends ReadOnlyState> stateAdapterClass) {
        try {
            stateAdapterConstructor = stateAdapterClass.getDeclaredConstructor(stateClass);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("No suitable constructor found on stateAdapterClass", e);
        }
    }

    protected void registerCommandHandler(Class<? extends Command> command, CommandHandler handler) {
        this.commandHandlers.put(command, handler);
    }

    protected void registerQueryHandler(Class<? extends Query> query, QueryHandler handler) {
        this.queryHandlers.put(query, handler);
    }

    protected void registerEventHandler(Class<? extends SourcedEvent> event, SourcedEventHandler handler) {
        this.eventHandlers.put(event, handler);
    }

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        // we can only get Commands or Queries here, anything else will be seen as an error
        if(message instanceof Command) {
            sender.tell(doHandleCommand((Command) message));
        } else if(message instanceof Query) {
            sender.tell(handleQuery((Query) message));
        } else {
            // @TODO return an error message to the sender
        }
    }

    protected CommandResponse doHandleCommand(Command command) {
        // create a new command context
        commandContextHolder.setContext(new CommandContext(command));
        CommandResponse responseMessage = executeCommand(command);
        CommandContext commandContext = commandContextHolder.getAndClearContext();
        commandContext.setCommandResponse(responseMessage);
        commitCommandContext(commandContext);
        return responseMessage;
    }

    private CommandResponse executeCommand(Command commandMessage) {
        CommandHandler commandHandler = commandHandlers.get(commandMessage.getClass());
        if(commandHandler != null) {
            return commandHandler.handle(commandMessage, adaptState(getState(stateClass)), getSystem());
        } else {
            // TODO: throw an error
            return null;
        }
    }

    protected abstract void commitCommandContext(CommandContext context);

    protected void apply(SourcedEvent event) {
        SourcedEventHandler eventHandler = eventHandlers.get(event.getClass());
        if(eventHandler != null) {
            eventHandler.update(event, getState(stateClass));
            commandContextHolder.getContext().addEvent(event);
        } else {
            // @TODO raise an error here
        }
    }

    private QueryResponse handleQuery(Query query) {
        QueryHandler queryHandler = queryHandlers.get(query.getClass());
        if(queryHandler != null) {
            return queryHandler.query(query, adaptState(getState(stateClass)), getSystem());
        } else {
            // @TODO return an error response here
            return null;
        }
    }

    @Override
    public boolean shouldUpdateState(Object message) {
        if(message instanceof Query) {
            return false;
        } else if(message instanceof Command) {
            return true;
        } else {
            // not sure if we want to allow message of another type, but return true to be on the safe side
            return true;
        }
    }

    @Override
    public boolean shouldUpdateState(ActorLifecycleStep lifecycleStep) {
        if(ActorLifecycleStep.CREATE.equals(lifecycleStep)) {
            return true;
        } else {
            return false;
        }
    }

    private Class<? extends ActorState> resolveActorStateClass() {
        Actor actorAnnotation = getClass().getAnnotation(Actor.class);
        if(actorAnnotation != null) {
            return actorAnnotation.stateClass();
        } else {
            TempActor tempActorAnnotation = this.getClass().getAnnotation(TempActor.class);
            if(tempActorAnnotation != null) {
                return tempActorAnnotation.stateClass();
            }
        }
        return null;
    }

    private ReadOnlyState adaptState(ActorState state) {
        try {
            return stateAdapterConstructor.newInstance(state);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
