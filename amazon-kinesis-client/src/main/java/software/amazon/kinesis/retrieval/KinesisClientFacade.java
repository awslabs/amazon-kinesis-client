package software.amazon.kinesis.retrieval;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.kinesis.common.KinesisRequestsBuilder;

/**
 * Facade pattern to simplify interactions with a {@link KinesisAsyncClient}.
 */
@Slf4j
public final class KinesisClientFacade {

    /**
     * Reusable {@link AWSExceptionManager}.
     * <p>
     * N.B. This instance is mutable, but thread-safe for <b>read-only</b> use.
     * </p>
     */
    private static final AWSExceptionManager AWS_EXCEPTION_MANAGER;

    // FIXME dependency injection
    private static KinesisAsyncClient kinesisClient;

    static {
        AWS_EXCEPTION_MANAGER = new AWSExceptionManager();
        AWS_EXCEPTION_MANAGER.add(KinesisException.class, t -> t);
        AWS_EXCEPTION_MANAGER.add(LimitExceededException.class, t -> t);
        AWS_EXCEPTION_MANAGER.add(ResourceInUseException.class, t -> t);
        AWS_EXCEPTION_MANAGER.add(ResourceNotFoundException.class, t -> t);
    }

    static void initialize(final KinesisAsyncClient client) {
        kinesisClient = client;
    }

    public static DescribeStreamSummaryResponse describeStreamSummary(final String streamArn) {
        final DescribeStreamSummaryRequest request = KinesisRequestsBuilder
                .describeStreamSummaryRequestBuilder().streamARN(streamArn).build();
        final ServiceCallerSupplier<DescribeStreamSummaryResponse> dss =
                () -> kinesisClient.describeStreamSummary(request).get();
        return retryWhenThrottled(dss, 3, streamArn, "DescribeStreamSummary");
    }

    // FIXME code lifted-and-shifted from FanOutConsumerRegistration; that class
    //      (and others) should not be responsible for interacting directly with
    //      the thread-safe Kinesis client (and handling retries, etc.)
    private static <T> T retryWhenThrottled(
            @NonNull final ServiceCallerSupplier<T> retriever,
            final int maxRetries,
            final String streamArn,
            @NonNull final String apiName) {
        LimitExceededException finalException = null;

        int retries = maxRetries;
        while (retries > 0) {
            try {
                try {
                    return retriever.get();
                } catch (ExecutionException e) {
                    throw AWS_EXCEPTION_MANAGER.apply(e.getCause());
                } catch (InterruptedException e) {
                    throw KinesisException.create("Unable to complete " + apiName, e);
                } catch (TimeoutException te) {
                    log.info("Timed out waiting for " + apiName + " for " + streamArn);
                }
            } catch (LimitExceededException e) {
                log.info("{} : Throttled while calling {} API, will backoff.", streamArn, apiName);
                try {
                    Thread.sleep(1000 + (long) (Math.random() * 100));
                } catch (InterruptedException ie) {
                    log.debug("Sleep interrupted, shutdown invoked.");
                }
                finalException = e;
            }
            retries--;
        }

        if (finalException == null) {
            throw new IllegalStateException(streamArn + " : Exhausted retries while calling " + apiName);
        }

        throw finalException;
    }

    @FunctionalInterface
    private interface ServiceCallerSupplier<T> {
        T get() throws ExecutionException, InterruptedException, TimeoutException;
    }

}
