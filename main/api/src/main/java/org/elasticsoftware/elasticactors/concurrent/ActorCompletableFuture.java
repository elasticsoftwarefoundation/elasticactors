/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors.concurrent;

import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ElasticActor;

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
import java.util.function.Supplier;

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

    /**
     * Constructs a new ActorCompletableFuture.
     *
     * Made private since 6.0.3. It was never meant to be public, thus this was considered a bugfix.
     * Use {@link ActorCompletableFuture#wrap(CompletableFuture)} instead.
     */
    private ActorCompletableFuture(CompletableFuture<T> delegate) {
        this.delegate = delegate;
    }

    /**
     * Wraps an existing {@link CompletableFuture} into an ActorCompletableFuture.
     * Please consider using one of the ActorCompletableFuture variations of the
     * static {@link CompletableFuture} methods such as:
     *
     * <ul>
     *  <li>{@link ActorCompletableFuture#supplyAsync(Supplier)}</li>
     *  <li>{@link ActorCompletableFuture#supplyAsync(Supplier, Executor)}</li>
     *  <li>{@link ActorCompletableFuture#runAsync(Runnable)}</li>
     *  <li>{@link ActorCompletableFuture#runAsync(Runnable, Executor)}</li>
     *  <li>{@link ActorCompletableFuture#completedFuture(Object)}</li>
     * </ul>
     *
     * @param completableFuture the {@link CompletableFuture} to wrap
     * @return a {@link CompletableFuture} wrapped in an ActorCompletableFuture,
     * or itself if it was already one.
     */
    public static <T> ActorCompletableFuture<T> wrap(CompletableFuture<T> completableFuture) {
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

    /**
     * The ActorCompletableFuture version of {@link CompletableFuture#supplyAsync(Supplier)}
     */
    public static <U> ActorCompletableFuture<U> supplyAsync(Supplier<U> supplier) {
        return wrap(CompletableFuture.supplyAsync(supplier));
    }

    /**
     * The ActorCompletableFuture version of {@link CompletableFuture#supplyAsync(Supplier, Executor)}
     */
    public static <U> ActorCompletableFuture<U> supplyAsync(Supplier<U> supplier, Executor executor) {
        return wrap(CompletableFuture.supplyAsync(supplier, executor));
    }

    /**
     * The ActorCompletableFuture version of {@link CompletableFuture#runAsync(Runnable)}
     */
    public static ActorCompletableFuture<Void> runAsync(Runnable runnable) {
        return wrap(CompletableFuture.runAsync(runnable));
    }

    /**
     * The ActorCompletableFuture version of {@link CompletableFuture#runAsync(Runnable, Executor)}
     */
    public static ActorCompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
        return wrap(CompletableFuture.runAsync(runnable, executor));
    }

    /**
     * The ActorCompletableFuture version of {@link CompletableFuture#completedFuture(Object)}
     */
    public static <U> ActorCompletableFuture<U> completedFuture(U value) {
        return wrap(CompletableFuture.completedFuture(value));
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    /**
     * A specialization of {@link CompletableFuture#get()} that throws an exception if called
     * when handling an actor message.
     *
     * @throws IllegalStateException if the method is called within a {@link ElasticActor} lifecycle or on(Message) method
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
     * @throws IllegalStateException if the method is called within a {@link ElasticActor} lifecycle or on(Message) method
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
     * @throws IllegalStateException if the method is called within a {@link ElasticActor} lifecycle or on(Message) method
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
        return wrap(delegate.thenCombine(unwrap(other), fn));
    }

    @Override
    public <U, V> ActorCompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn)
    {
        return wrap(delegate.thenCombineAsync(unwrap(other), fn));
    }

    @Override
    public <U, V> ActorCompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> other,
        BiFunction<? super T, ? super U, ? extends V> fn,
        Executor executor)
    {
        return wrap(delegate.thenCombineAsync(unwrap(other), fn, executor));
    }

    @Override
    public <U> ActorCompletableFuture<Void> thenAcceptBoth(
        CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action)
    {
        return wrap(delegate.thenAcceptBoth(unwrap(other), action));
    }

    @Override
    public <U> ActorCompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action)
    {
        return wrap(delegate.thenAcceptBothAsync(unwrap(other), action));
    }

    @Override
    public <U> ActorCompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> other,
        BiConsumer<? super T, ? super U> action,
        Executor executor)
    {
        return wrap(delegate.thenAcceptBothAsync(unwrap(other), action, executor));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterBoth(
        CompletionStage<?> other, Runnable action)
    {
        return wrap(delegate.runAfterBoth(unwrap(other), action));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterBothAsync(
        CompletionStage<?> other, Runnable action)
    {
        return wrap(delegate.runAfterBothAsync(unwrap(other), action));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterBothAsync(
        CompletionStage<?> other, Runnable action, Executor executor)
    {
        return wrap(delegate.runAfterBothAsync(unwrap(other), action, executor));
    }

    @Override
    public <U> ActorCompletableFuture<U> applyToEither(
        CompletionStage<? extends T> other, Function<? super T, U> fn)
    {
        return wrap(delegate.applyToEither(unwrap(other), fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> applyToEitherAsync(
        CompletionStage<? extends T> other, Function<? super T, U> fn)
    {
        return wrap(delegate.applyToEitherAsync(unwrap(other), fn));
    }

    @Override
    public <U> ActorCompletableFuture<U> applyToEitherAsync(
        CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor)
    {
        return wrap(delegate.applyToEitherAsync(unwrap(other), fn, executor));
    }

    @Override
    public ActorCompletableFuture<Void> acceptEither(
        CompletionStage<? extends T> other, Consumer<? super T> action)
    {
        return wrap(delegate.acceptEither(unwrap(other), action));
    }

    @Override
    public ActorCompletableFuture<Void> acceptEitherAsync(
        CompletionStage<? extends T> other, Consumer<? super T> action)
    {
        return wrap(delegate.acceptEitherAsync(unwrap(other), action));
    }

    @Override
    public ActorCompletableFuture<Void> acceptEitherAsync(
        CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor)
    {
        return wrap(delegate.acceptEitherAsync(unwrap(other), action, executor));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterEither(
        CompletionStage<?> other, Runnable action)
    {
        return wrap(delegate.runAfterEither(unwrap(other), action));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterEitherAsync(
        CompletionStage<?> other, Runnable action)
    {
        return wrap(delegate.runAfterEitherAsync(unwrap(other), action));
    }

    @Override
    public ActorCompletableFuture<Void> runAfterEitherAsync(
        CompletionStage<?> other, Runnable action, Executor executor)
    {
        return wrap(delegate.runAfterEitherAsync(unwrap(other), action, executor));
    }

    @Override
    public <U> ActorCompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return wrap(delegate.thenCompose(fn.andThen(ActorCompletableFuture::unwrap)));
    }

    @Override
    public <U> ActorCompletableFuture<U> thenComposeAsync(Function<? super T, ?
        extends CompletionStage<U>> fn) {
        return wrap(delegate.thenComposeAsync(fn.andThen(ActorCompletableFuture::unwrap)));
    }

    @Override
    public <U> ActorCompletableFuture<U> thenComposeAsync(
        Function<? super T, ? extends CompletionStage<U>> fn, Executor executor)
    {
        return wrap(delegate.thenComposeAsync(fn.andThen(ActorCompletableFuture::unwrap), executor));
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

    private static <T> CompletionStage<T> unwrap(CompletionStage<T> stage) {
        while (stage instanceof ActorCompletableFuture) {
            stage = ((ActorCompletableFuture<T>) stage).delegate;
        }
        return stage;
    }
}
