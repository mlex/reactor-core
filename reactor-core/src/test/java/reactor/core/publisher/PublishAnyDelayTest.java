package reactor.core.publisher;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import reactor.core.scheduler.Schedulers;

public class PublishAnyDelayTest {

  @Test
  public void delayElementShouldNotCancelTwice() throws Exception {
    DirectProcessor<Long> p = DirectProcessor.create();

    Flux<Long> publishedFlux = p
        .publish()
        .refCount(2);

    publishedFlux
        .any(x -> x > 5)
        .delayElement(Duration.ofMillis(2))
        .subscribe();

    CompletableFuture<List<Long>> result = publishedFlux.collectList().toFuture();

    for (long i = 0; i < 10; i++) {
      p.onNext(i);
      Thread.sleep(1);
    }
    p.onComplete();

    assertEquals(10, result.get(10, TimeUnit.MILLISECONDS).size());
  }
}
