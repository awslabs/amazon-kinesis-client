package software.amazon.kinesis.worker.platform;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Ec2ResourceTest {

    private UrlOpener mockIdUrl;
    private UrlOpener mockTokenUrl;
    private HttpURLConnection mockIdConnection;
    private HttpURLConnection mockTokenConnection;
    private Ec2Resource ec2Resource;

    @BeforeEach
    void setUp() throws Exception {
        mockIdUrl = mock(UrlOpener.class);
        mockTokenUrl = mock(UrlOpener.class);
        mockIdConnection = mock(HttpURLConnection.class);
        mockTokenConnection = mock(HttpURLConnection.class);
        ec2Resource = new Ec2Resource(mockIdUrl, mockTokenUrl);
        when(mockIdUrl.openConnection()).thenReturn(mockIdConnection);
        when(mockTokenUrl.openConnection()).thenReturn(mockTokenConnection);
    }

    @Test
    void testIsEc2WhenResponseCode200() throws Exception {
        when(mockIdConnection.getResponseCode()).thenReturn(200);
        assertTrue(ec2Resource.isOnPlatform());
        assertEquals(ResourceMetadataProvider.ComputePlatform.EC2, ec2Resource.getPlatform());
    }

    @Test
    void testIsEc2WhenTokenConnectionThrowsBecauseImdsV1() throws Exception {
        when(mockTokenConnection.getResponseCode()).thenThrow(new IOException());
        when(mockIdConnection.getResponseCode()).thenReturn(200);
        assertTrue(ec2Resource.isOnPlatform());
    }

    @Test
    void testIsNotEc2() throws Exception {
        when(mockIdConnection.getResponseCode()).thenReturn(403);
        assertFalse(ec2Resource.isOnPlatform());
    }

    @Test
    void testIsNotEc2WhenConnectionThrows() throws Exception {
        when(mockIdConnection.getResponseCode()).thenThrow(new IOException());
        assertFalse(ec2Resource.isOnPlatform());
    }
}
