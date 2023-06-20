package software.amazon.kinesis.utils;

import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AWSResourceManager {

    public AWSResourceManager() {}

    public abstract void _deleteResource(String resourceName) throws Exception;
    public abstract boolean _isResourceActive(String name);
    public abstract List<String> _getAllResourceNames() throws Exception;

    /**
     * Deletes resource with specified resource name
     * @param resourceName
     * @throws Exception
     */
    public void deleteResource(String resourceName) throws Exception {

        try {
            _deleteResource(resourceName);
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
                if (!_isResourceActive(resourceName)) {
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
        final List<String> streamNames = _getAllResourceNames();
        for (String streamName : streamNames) {
            deleteResource(streamName);
        }
    }
}
