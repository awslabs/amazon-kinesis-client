package com.amazonaws.services.kinesis.clientlibrary.utils;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import static java.lang.Thread.sleep;

@Slf4j
@NoArgsConstructor
public abstract class AWSResourceManager {

    public abstract void deleteResourceCall(String resourceName) throws Exception;
    public abstract boolean isResourceActive(String name);
    public abstract List<String> getAllResourceNames() throws Exception;

    /**
     * Deletes resource with specified resource name
     * @param resourceName
     * @throws Exception
     */
    public void deleteResource(String resourceName) throws Exception {

        try{
            deleteResourceCall(resourceName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete resource with name " + resourceName, e);
        }

        int i = 0;
        while (true) {
            i++;
            if (i > 100) {
                throw new RuntimeException("Failed to delete resource");
            }
            try {
                if (!isResourceActive(resourceName)) {
                    log.info("Succesfully deleted the resource " + resourceName);
                    return;
                }
            } catch (Exception e) {
                try {
                    sleep(10_000); // 10 secs backoff.
                } catch (InterruptedException e1) {}
                log.info("Resource {} is not deleted yet, exception: ", resourceName, e);
            }
        }
    }

    /**
     * Delete all instances of a particular resource type
     */
    public void deleteAllResource() throws Exception {

        List<String> resourceNames = getAllResourceNames();
        for(String resourceName: resourceNames) {
            deleteResource(resourceName);
        }
    }
}
