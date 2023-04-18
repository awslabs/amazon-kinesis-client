package software.amazon.kinesis.common;

import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;

import org.junit.Test;

public class StreamConfigTest {

    @Test(expected = NullPointerException.class)
    public void testNullStreamIdentifier() {
        new StreamConfig(null, InitialPositionInStreamExtended.newInitialPosition(TRIM_HORIZON));
    }

}