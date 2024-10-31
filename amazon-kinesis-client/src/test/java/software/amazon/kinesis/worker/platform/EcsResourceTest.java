package software.amazon.kinesis.worker.platform;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EcsResourceTest {

    private static final String ECS_METADATA_KEY_V4 = "ECS_CONTAINER_METADATA_URI_V4";
    private static final String ECS_METADATA_KEY_V3 = "ECS_CONTAINER_METADATA_URI";

    @Test
    void testIsEcs() {
        final Map<String, String> mockSysEnv = new HashMap<>();
        mockSysEnv.put(ECS_METADATA_KEY_V3, "v3");
        assertTrue(new EcsResource(mockSysEnv).isOnPlatform());

        // test when both v3 and v4 exists
        mockSysEnv.put(ECS_METADATA_KEY_V4, "v4");
        assertTrue(new EcsResource(mockSysEnv).isOnPlatform());

        // test when only v4 exists
        mockSysEnv.remove(ECS_METADATA_KEY_V3);
        final EcsResource ecsResource = new EcsResource(mockSysEnv);
        assertTrue(ecsResource.isOnPlatform());
        assertEquals(ResourceMetadataProvider.ComputePlatform.ECS, ecsResource.getPlatform());
    }

    @Test
    void testIsNotEcs() {
        final Map<String, String> mockSysEnv = new HashMap<>();
        assertFalse(new EcsResource(mockSysEnv).isOnPlatform());

        mockSysEnv.put(ECS_METADATA_KEY_V3, "");
        mockSysEnv.put(ECS_METADATA_KEY_V4, "");
        assertFalse(new EcsResource(mockSysEnv).isOnPlatform());
    }
}
