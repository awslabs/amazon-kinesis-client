package software.amazon.kinesis.utils;

import software.amazon.kinesis.config.KCLAppConfig;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@NoArgsConstructor
public abstract class AWSResourceManager {

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

        // Wait till resource is deleted to return
        int i = 0;
        while (true) {
            i++;
            if (i > 100) {
                throw new RuntimeException("Failed resource deletion");
            }
            try {
                if (!isResourceActive(resourceName)) {
                    log.info("Successfully deleted the resource {}", resourceName);
                    return;
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                } catch (InterruptedException e1) {}
                log.info("Resource {} is not deleted yet, exception: ", resourceName);
            }
        }
    }

    /**
     * Delete all instances of a particular resource type
     */
    public void deleteAllResource() throws Exception {
        final List<String> resourceNames = getAllResourceNames();
        for (String resourceName : resourceNames) {
            // Delete all resources that have prefix "KCLRelease"
            if (resourceName.startsWith(KCLAppConfig.INTEGRATION_TEST_RESOURCE_PREFIX)) {
                deleteResource(resourceName);
            }
        }
    }
}
