package software.amazon.kinesis.common;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import software.amazon.kinesis.utils.RecordValidatorQueue;

public class RecordValidatorQueueTest {

    @Mock
    private RecordValidatorQueue recordValidator;

    private static final String shardId = "ABC";

    private final int outOfOrderError = -1;
    private final int missingRecordError = -2;

    private final int noError = 0;

    @Test
    public void validationFailedRecordOutOfOrderTest() {
        recordValidator = new RecordValidatorQueue();
        recordValidator.add(shardId, "0");
        recordValidator.add(shardId, "1");
        recordValidator.add(shardId, "3");
        recordValidator.add(shardId, "2");

        int error = recordValidator.validateRecords( 4 );
        Assert.assertEquals(outOfOrderError, error);
    }

    @Test
    public void validationFailedMissingRecordTest() {
        recordValidator = new RecordValidatorQueue();
        recordValidator.add(shardId, "0");
        recordValidator.add(shardId, "1");
        recordValidator.add(shardId, "2");
        recordValidator.add(shardId, "3");

        int error = recordValidator.validateRecords( 5 );
        Assert.assertEquals(missingRecordError, error);
    }

    @Test
    public void validRecordsTest() {
        recordValidator = new RecordValidatorQueue();
        recordValidator.add(shardId, "0");
        recordValidator.add(shardId, "1");
        recordValidator.add(shardId, "2");
        recordValidator.add(shardId, "3");

        int error = recordValidator.validateRecords( 4 );
        Assert.assertEquals(noError, error);
    }
}
