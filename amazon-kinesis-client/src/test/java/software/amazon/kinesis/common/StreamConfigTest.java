package software.amazon.kinesis.common;

import org.junit.Test;

import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;

public class StreamConfigTest {

    @Test(expected = NullPointerException.class)
    public void testNullStreamIdentifier() {
        new StreamConfig(null, InitialPositionInStreamExtended.newInitialPosition(TRIM_HORIZON));
    }
}
