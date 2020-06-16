package reactor.core.publisher;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import reactor.core.scheduler.Schedulers;

class PublishOnThreadStealingTest {

    @Test
    public void reproducer() throws Throwable {
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Flux.<Integer>create(emitter -> infiniteFastProducer(emitter), FluxSink.OverflowStrategy.BUFFER)
                .take(Duration.ofSeconds(5))
                .publishOn(Schedulers.newParallel("process"))
                .flatMapIterable(this::process)
                .publishOn(Schedulers.newParallel("downstream"), Schedulers.newParallel("request"), true, 256)
                .subscribe(
                        x -> LockSupport.parkNanos(1_000),
                        e -> {
                            throwable.set(e);
                            latch.countDown();
                        },
                        () -> latch.countDown());

        latch.await();
        if (throwable.get() != null)
            throw throwable.get();
    }

    private List<Integer> process(Integer x) {
        LockSupport.parkNanos(100_000);
        if (Thread.currentThread().getName().contains("downstream")) {
            final RuntimeException e = new RuntimeException("This should not run on a 'downstream' thread");
            e.printStackTrace();
            throw e;
        }
        return IntStream.range(0, 100).map(i -> i*x).boxed().collect(Collectors.toList());
    }

    private void infiniteFastProducer(FluxSink<Integer> emitter) {
        final AtomicBoolean stopped = new AtomicBoolean(false);
        new Thread(() -> {
            int i = 0;
            while (!stopped.get()) {
                LockSupport.parkNanos(1_000);
                if (emitter.requestedFromDownstream() > 0)
                    emitter.next(++i);
            }
        }).start();
        emitter.onCancel(() -> stopped.set(true));
        emitter.onDispose(() -> stopped.set(true));
    }

}