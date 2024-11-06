package software.amazon.kinesis.worker.platform;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EksResourceTest {

    @TempDir
    Path tempDir;

    @Test
    void testIsEks() throws IOException {
        final File mockK8sTokenFile = new File(tempDir.toFile(), "k8sToken");
        mockK8sTokenFile.createNewFile();
        final EksResource eksResource = new EksResource(mockK8sTokenFile.getPath());
        assertTrue(eksResource.isOnPlatform());
        assertEquals(ResourceMetadataProvider.ComputePlatform.EKS, eksResource.getPlatform());
    }

    @Test
    void testIsNotEks() {
        final EksResource eksResource = new EksResource("");
        assertFalse(eksResource.isOnPlatform());
    }
}
