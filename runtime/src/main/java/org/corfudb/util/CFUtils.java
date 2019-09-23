package org.corfudb.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by mwei on 9/15/15.
 */
public final class CFUtils {
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("failAfter-%d")
                            .build());

    /**
     * A static timeout exception that we complete futures exceptionally with.
     */
    static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();

    private CFUtils() {
        // Prevent initializing a utility class
    }

    @SuppressWarnings("unchecked")
    public static <T,
            A extends Throwable,
            B extends Throwable,
            C extends Throwable,
            D extends Throwable> T getUninterruptibly(Future<T> future,
                                                      Class<A> throwableA,
                                                      Class<B> throwableB,
                                                      Class<C> throwableC,
                                                      Class<D> throwableD)
            throws A, B, C, D {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError("Interrupted while completing future", e);
        } catch (ExecutionException ee) {
            if (throwableA.isInstance(ee.getCause())) {
                throw (A) ee.getCause();
            }
            if (throwableB.isInstance(ee.getCause())) {
                throw (B) ee.getCause();
            }
            if (throwableC.isInstance(ee.getCause())) {
                throw (C) ee.getCause();
            }
            if (throwableD.isInstance(ee.getCause())) {
                throw (D) ee.getCause();
            }
            throw new RuntimeException(ee.getCause());
        }
    }

    public static <T,
            A extends Throwable,
            B extends Throwable,
            C extends Throwable> T getUninterruptibly(Future<T> future,
                                                      Class<A> throwableA,
                                                      Class<B> throwableB,
                                                      Class<C> throwableC)
            throws A, B, C {
        return getUninterruptibly(future, throwableA, throwableB, throwableC,
                RuntimeException.class);
    }

    public static <T,
            A extends Throwable,
            B extends Throwable> T getUninterruptibly(Future<T> future,
                                                      Class<A> throwableA,
                                                      Class<B> throwableB)
            throws A, B {
        return getUninterruptibly(future, throwableA, throwableB, RuntimeException.class,
                RuntimeException.class);
    }

    public static <T, A extends Throwable> T getUninterruptibly(Future<T> future,
                                                                Class<A> throwableA)
            throws A {
        return getUninterruptibly(future, throwableA, RuntimeException.class,
                RuntimeException.class, RuntimeException.class);
    }

    public static <T> T getUninterruptibly(Future<T> future) {
        return getUninterruptibly(future, RuntimeException.class, RuntimeException.class,
                RuntimeException.class, RuntimeException.class);
    }

    /**
     * Generates a completable future which times out.
     * inspired by NoBlogDefFound: http://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     *
     * @param duration The duration to timeout after.
     * @param <T>      Ignored, since the future will always timeout.
     * @return A completable future that will time out.
     */
    public static <T> CompletableFuture<T> failAfter(Duration duration) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        SCHEDULER.schedule(() -> promise.completeExceptionally(TIMEOUT_EXCEPTION),
                duration.toMillis(), TimeUnit.MILLISECONDS);
        return promise;
    }

    /**
     * Schedules a runnable after a given time
     *
     * @param duration The duration to timeout after.
     */
    public static void runAfter(Duration duration, Runnable toRun) {
        SCHEDULER.schedule(toRun::run, duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Takes a completable future, and ensures that it completes within a certain duration.
     * If it does not, it is cancelled and completes exceptionally with TimeoutException.
     * inspired by NoBlogDefFound: www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
     *
     * @param future   The completable future that must be completed within duration.
     * @param duration The duration the future must be completed in.
     * @param <T>      The return type of the future.
     * @return A completable future which completes with the original value if completed
     * within duration, otherwise completes exceptionally with TimeoutException.
     */
    public static <T> CompletableFuture<T> within(CompletableFuture<T> future, Duration duration) {
        final CompletableFuture<T> timeout = failAfter(duration);
        return future.applyToEither(timeout, Function.identity());
    }

    public static <T> CompletableFuture<Void> allOf(Collection<CompletableFuture<T>> futures) {
        CompletableFuture<T>[] futuresArr = futures.toArray(new CompletableFuture[futures.size()]);
        return CompletableFuture.allOf(futuresArr);
    }

    /**
     * Takes a list of completable futures and returns a CompletableFuture of a list.
     *
     * @param futures A list of completable futures, perhaps a result of a map function.
     * @param <T>     A return type of the future.
     * @return A completable future, which completes with a list of the results.
     */
    public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
        return CompletableFuture
                .allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
                .thenApply(x -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }

    /**
     *
     * @param executor ScheduledExecutorService instance to schedule a task.
     * @param command  A supplier that represents an executable task.
     * @param delay Delay before the task is executed.
     * @param unit  The units of the delay.
     * @param <T> A return type of the future.
     * @return A completable future, which completes after delay units.
     */
    public static <T> CompletableFuture<T> schedule(
            ScheduledExecutorService executor,
            Supplier<T> command,
            long delay,
            TimeUnit unit
    ) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        executor.schedule(
                (() -> {
                    try {
                        return completableFuture.complete(command.get());
                    } catch (Throwable t) {
                        return completableFuture.completeExceptionally(t);
                    }
                }),
                delay,
                unit
        );
        return completableFuture;
    }

    /**
     * Unwraps ExecutionException thrown from a CompletableFuture.
     *
     * @param throwable  Throwable to unwrap.
     * @param throwableA Checked Exception to expose.
     * @param <A>        Class of checked exception.
     * @throws A Throws checked exception.
     */
    public static <A extends Throwable> void unwrap(Throwable throwable, Class<A> throwableA) throws A {

        Throwable unwrapThrowable = throwable;
        if (throwable instanceof ExecutionException || throwable instanceof CompletionException) {
            unwrapThrowable = throwable.getCause();
        }

        if (throwableA.isInstance(unwrapThrowable)) {
            throw (A) unwrapThrowable;
        }
        if (unwrapThrowable instanceof RuntimeException) {
            throw (RuntimeException) unwrapThrowable;
        }
        if (unwrapThrowable instanceof Error) {
            throw (Error) unwrapThrowable;
        }
        throw new RuntimeException(unwrapThrowable);
    }

    /**
     * Unwraps ExecutionException thrown from a CompletableFuture.
     *
     * @param throwable Throwable to unwrap.
     */
    public static void unwrap(Throwable throwable) {
        unwrap(throwable, RuntimeException.class);
    }


}
