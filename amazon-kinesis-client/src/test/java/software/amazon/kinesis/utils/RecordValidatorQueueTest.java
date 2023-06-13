package software.amazon.kinesis.utils;

import org.junit.Assert;
import org.junit.Test;

public class RecordValidatorQueueTest {

    private RecordValidatorQueue recordValidator = new RecordValidatorQueue();

    private static final String SHARD_ID = "ABC";

    @Test
    public void validationFailedRecordOutOfOrderTest() {
        recordValidator.add(SHARD_ID, "0");
        recordValidator.add(SHARD_ID, "1");
        recordValidator.add(SHARD_ID, "3");
        recordValidator.add(SHARD_ID, "2");

        RecordValidationStatus error = recordValidator.validateRecords(4);
        Assert.assertEquals(RecordValidationStatus.OUT_OF_ORDER, error);
    }

    @Test
    public void validationFailedMissingRecordTest() {
        recordValidator.add(SHARD_ID, "0");
        recordValidator.add(SHARD_ID, "1");
        recordValidator.add(SHARD_ID, "2");
        recordValidator.add(SHARD_ID, "3");

        RecordValidationStatus error = recordValidator.validateRecords(5);
        Assert.assertEquals(RecordValidationStatus.MISSING_RECORD, error);
    }

    @Test
    public void validRecordsTest() {
        recordValidator.add(SHARD_ID, "0");
        recordValidator.add(SHARD_ID, "1");
        recordValidator.add(SHARD_ID, "2");
        recordValidator.add(SHARD_ID, "3");

        RecordValidationStatus error = recordValidator.validateRecords(4);
        Assert.assertEquals(RecordValidationStatus.NO_ERROR, error);
    }
}
