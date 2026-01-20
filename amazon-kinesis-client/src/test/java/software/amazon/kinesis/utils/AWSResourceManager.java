package software.amazon.kinesis.utils;

import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public abstract class AWSResourceManager {

    private static final int RESOURCE_DELETION_CHECK_MAX_RETRIES = 10;
    private static final int RESOURCE_DELETION_CHECK_SLEEP_TIME_SECONDS = 10;

    /**
     * Make delete resource API call for specific resource type
     */
    public abstract void deleteResourceCall(String resourceName) throws Exception;

    /**
     * Check if resource with given name is in active state
     */
    public abstract boolean isResourceActive(String name);

    /**
     * Get a list of all the names of resources of a specified type
     *
     * @throws Exception
     */
    public abstract List<String> getAllResourceNames() throws Exception;

    /**
     * Delete resource with specified resource name
     */
    public void deleteResource(String resourceName) throws Exception {

        try {
            deleteResourceCall(resourceName);
        } catch (Exception e) {
            throw new Exception("Could not delete resource: {}", e);
        }

        Exception lastException = null;
        int retries = 0;
        do {
            try {
                if (!isResourceActive(resourceName)) {
                    log.info("Successfully deleted the resource {}", resourceName);
                    return;
                }
            } catch (Exception e) {
                lastException = e;
                log.info(
                        "Retry {}/{}: Resource {} not yet deleted. Checking again in {} seconds.",
                        retries,
                        RESOURCE_DELETION_CHECK_MAX_RETRIES,
                        resourceName,
                        RESOURCE_DELETION_CHECK_SLEEP_TIME_SECONDS);
                sleepAndSwallowException();
            }
        } while (retries++ < RESOURCE_DELETION_CHECK_MAX_RETRIES);
        throw new RuntimeException("Could not delete resource: " + resourceName + ". Last exception: " + lastException);
    }

    private void sleepAndSwallowException() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(RESOURCE_DELETION_CHECK_SLEEP_TIME_SECONDS));
        } catch (InterruptedException e) {
            log.info("Interrupted while waiting for resource to be deleted.");
        }
    }

    /**
     * Delete all instances of a particular resource type
     */
    public void deleteAllResource(final String prefix) throws Exception {
        final List<String> resourceNames = getAllResourceNames();
        for (String resourceName : resourceNames) {
            if (resourceName.startsWith(prefix)) {
                deleteResource(resourceName);
            }
        }
    }
}
