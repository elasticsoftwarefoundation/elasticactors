package org.elasticsoftware.elasticactors.cqrs;

public class CommandContextHolder {
    private final ThreadLocal<CommandContext> threadContext = new ThreadLocal<>();

    public CommandContext setContext(CommandContext context) {
        final CommandContext currentContext = threadContext.get();
        threadContext.set(context);
        return currentContext;
    }

    public CommandContext getContext() {
        return threadContext.get();
    }

    public CommandContext getAndClearContext() {
        final CommandContext currentContext = threadContext.get();
        threadContext.set(null);
        return currentContext;
    }
}
