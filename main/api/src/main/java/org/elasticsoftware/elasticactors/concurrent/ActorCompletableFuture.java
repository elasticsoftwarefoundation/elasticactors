package org.elasticsoftware.elasticactors.concurrent;

import org.elasticsoftware.elasticactors.ActorContextHolder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A specialization of {@link CompletableFuture} that doesn't allow synchronous operations while
 * handling messages for an Actor.
 *
 * Those operations could cause a deadlock due to actors being handled by one and only one
 * specific thread during the life of a node. Waiting for another actor could potentially mean
 * waiting for the current thread to handle another actor's messages, but the thread would be
 * blocked by this actor, thus creating a deadlock.
 *
 * Calling any of these operations in an object of this class in the context of an Actor will
 * cause an exception to be thrown.
 */
public final class ActorCompletableFuture<T> extends CompletableFuture<T> {

    private final CompletableFuture<T> delegate;

    public ActorCompletableFuture() {
        this(new CompletableFuture<>());
    }

    public ActorCompletableFuture(CompletableFuture<T> delegate) {
        this.delegate = delegate;
    }

    private static <T> ActorCompletableFuture<T> wrap(CompletableFuture<T> completableFuture) {
        if (completableFuture instanceof ActorCompletableFuture) {
            return (ActorCompletableFuture<T>) completableFuture;
        }
        return new ActorCompletableFuture<>(completableFuture);
    }

    private static void checkNoActorContext() {
        if (ActorContextHolder.hasActorContext()) {
            throw new IllegalStateException(String.format(
                "Cannot perform synchronous operations of an ActorCompletableFuture inside an "
                    + "actor context. Current actor: [%s]",
                ActorContextHolder.getSelf()
            ));
        }
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    /**
     * A specialization of {@link CompletableFuture#get()} that throws an exception if called
     * when handling an actor message.
     *
     * @see CompletableFuture#get()
     */
    @Override
    public T get() throws InterruptedException, ExecutionException {
        checkNoActorContext();
        return delegate.get();
    }

    /**
     * A specialization of {@link CompletableFuture#get(long, TimeUnit)} that throws an exception
     * if called when handling an actor message.
     *
     * @see CompletableFuture#get(long, TimeUnit)
     */
    @Override
    public T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
        checkNoActorContext();
        return delegate.get(timeout, unit);
    }

    /**
     * A specialization of {@link CompletableFuture#join()} that throws an exception if called
     * when handling an actor message.
     *
     * @see CompletableFuture#join()
     */
    @Override
    public T join() {
        checkNoActorContext();
        return delegate.join();
    }

    @Override
    public T getNow(T valueIfAbsent) {
        return delegate.getNow(valueIfAbsent);
    }

    @Override
    public boolean complete(T value) {
        return delegate.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return delegate.completeExceptionally(ex);
    }

    @Override
    public <U> ActorCompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return wrap(delegate.thenApply(fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return wrap(delegate.thenApplyAsync(fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> thenApplyAsync(
        Function<? super T, ? extends U> fn, Executor executor)
    {
        return wrap(delegate.thenApplyAsync(fn, executor));
    }

    @Override
    public ActorCompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return wrap(delegate.thenAccept(action));
    }

    @Override
    public ActorCompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
        return wrap(delegate.thenAcceptAsync(action));
    }

    @Override
    public ActorCompletableFuture<Void> thenAcceptAsync(
        Consumer<? super T> action, Executor executor)
    {
        return wrap(delegate.thenAcceptAsync(action, executor));
    }

    @Override
    public ActorCompletableFuture<Void> thenRun(Runnable action) {
        return wrap(delegate.thenRun(action));
    }

    @Override
    public ActorCompletableFuture<Void> thenRunAsync(Runnable action) {
        return wrap(delegate.thenRunAsync(action));
    }

    @Override
    public ActorCompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return wrap(delegate.thenRunAsync(action, executor));
    }

    @Override
    public <U, V> ActorCompletableFuture<V> thenCombine(
        CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn)
    {
        return wrap(delegate.thenCombine(other, fn));
    }

    @Override
    public <U, V> ActorCompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn)
    {
        return wrap(delegate.thenCombineAsync(other, fn));
    }

    @Override
    public <U, V> ActorCompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> other,
        BiFunction<? super T, ? super U, ? extends V> fn,
        Executor executor)
    {
        return wrap(delegate.thenCombineAsync(other, fn, executor));
    }

    @Override
    public <U> ActorCompletableFuture<Void> thenAcceptBoth(
        CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action)
    {
        return wrap(delegate.thenAcceptBoth(other, action));
    }

    @Override
    public <U> ActorCompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action)
    {
        return wrap(delegate.thenAcceptBothAsync(other, action));
    }

    @Override
    public <U> ActorCompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> other,
        BiConsumer<? super T, ? super U> action,
        Executor executor)
    {
        return wrap(delegate.thenAcceptBothAsync(other, action, executor));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterBoth(
        CompletionStage<?> other, Runnable action)
    {
        return wrap(delegate.runAfterBoth(other, action));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterBothAsync(
        CompletionStage<?> other, Runnable action)
    {
        return wrap(delegate.runAfterBothAsync(other, action));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterBothAsync(
        CompletionStage<?> other, Runnable action, Executor executor)
    {
        return wrap(delegate.runAfterBothAsync(other, action, executor));
    }

    @Override
    public <U> ActorCompletableFuture<U> applyToEither(
        CompletionStage<? extends T> other, Function<? super T, U> fn)
    {
        return wrap(delegate.applyToEither(other, fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> applyToEitherAsync(
        CompletionStage<? extends T> other, Function<? super T, U> fn)
    {
        return wrap(delegate.applyToEitherAsync(other, fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> applyToEitherAsync(
        CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor)
    {
        return wrap(delegate.applyToEitherAsync(other, fn, executor));
    }

    @Override
    public ActorCompletableFuture<Void> acceptEither(
        CompletionStage<? extends T> other, Consumer<? super T> action)
    {
        return wrap(delegate.acceptEither(other, action));
    }

    @Override
    public ActorCompletableFuture<Void> acceptEitherAsync(
        CompletionStage<? extends T> other, Consumer<? super T> action)
    {
        return wrap(delegate.acceptEitherAsync(other, action));
    }

    @Override
    public ActorCompletableFuture<Void> acceptEitherAsync(
        CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor)
    {
        return wrap(delegate.acceptEitherAsync(other, action, executor));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterEither(
        CompletionStage<?> other, Runnable action)
    {
        return wrap(delegate.runAfterEither(other, action));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterEitherAsync(
        CompletionStage<?> other, Runnable action)
    {
        return wrap(delegate.runAfterEitherAsync(other, action));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterEitherAsync(
        CompletionStage<?> other, Runnable action, Executor executor)
    {
        return wrap(delegate.runAfterEitherAsync(other, action, executor));
    }

    @Override
    public <U> ActorCompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return wrap(delegate.thenCompose(fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> thenComposeAsync(Function<? super T, ?
        extends CompletionStage<U>> fn) {
        return wrap(delegate.thenComposeAsync(fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> thenComposeAsync(
        Function<? super T, ? extends CompletionStage<U>> fn, Executor executor)
    {
        return wrap(delegate.thenComposeAsync(fn, executor));
    }

    @Override
    public ActorCompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return wrap(delegate.whenComplete(action));
    }

    @Override
    public ActorCompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return wrap(delegate.whenCompleteAsync(action));
    }

    @Override
    public ActorCompletableFuture<T> whenCompleteAsync(
        BiConsumer<? super T, ? super Throwable> action, Executor executor)
    {
        return wrap(delegate.whenCompleteAsync(action, executor));
    }

    @Override
    public <U> ActorCompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return wrap(delegate.handle(fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return wrap(delegate.handleAsync(fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> handleAsync(
        BiFunction<? super T, Throwable, ? extends U> fn, Executor executor)
    {
        return wrap(delegate.handleAsync(fn, executor));
    }

    @Override
    public ActorCompletableFuture<T> toCompletableFuture() {
        return this;
    }

    @Override
    public ActorCompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return wrap(delegate.exceptionally(fn));
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isCompletedExceptionally() {
        return delegate.isCompletedExceptionally();
    }

    @Override
    public void obtrudeValue(T value) {
        delegate.obtrudeValue(value);
    }

    @Override
    public void obtrudeException(Throwable ex) {
        delegate.obtrudeException(ex);
    }

    @Override
    public int getNumberOfDependents() {
        return delegate.getNumberOfDependents();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
