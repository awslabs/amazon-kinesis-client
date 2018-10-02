/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.checkpoint;

//@RunWith(MockitoJUnitRunner.class)
public class SequenceNumberValidatorTest {
    /*private final String streamName = "testStream";
    private final boolean validateWithGetIterator = true;
    private final String shardId = "shardid-123";

    @Mock
    private AmazonKinesis amazonKinesis;

    @Test (expected = IllegalArgumentException.class)
    public final void testSequenceNumberValidator() {
        Checkpoint.SequenceNumberValidator validator = new Checkpoint.SequenceNumberValidator(amazonKinesis, streamName,
                shardId, validateWithGetIterator);

        String goodSequence = "456";
        String iterator = "happyiterator";
        String badSequence = "789";

        ArgumentCaptor<GetShardIteratorRequest> requestCaptor = ArgumentCaptor.forClass(GetShardIteratorRequest.class);

        when(amazonKinesis.getShardIterator(requestCaptor.capture()))
                .thenReturn(new GetShardIteratorResult().withShardIterator(iterator))
                .thenThrow(new InvalidArgumentException(""));

        validator.validateSequenceNumber(goodSequence);
        try {
            validator.validateSequenceNumber(badSequence);
        } finally {
            final List<GetShardIteratorRequest> requests = requestCaptor.getAllValues();
            assertEquals(2, requests.size());

            final GetShardIteratorRequest goodRequest = requests.get(0);
            final GetShardIteratorRequest badRequest = requests.get(0);

            assertEquals(streamName, goodRequest.getStreamName());
            assertEquals(shardId, goodRequest.shardId());
            assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), goodRequest.getShardIteratorType());
            assertEquals(goodSequence, goodRequest.getStartingSequenceNumber());

            assertEquals(streamName, badRequest.getStreamName());
            assertEquals(shardId, badRequest.shardId());
            assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), badRequest.getShardIteratorType());
            assertEquals(goodSequence, badRequest.getStartingSequenceNumber());
        }
    }

    @Test
    public final void testNoValidation() {
        Checkpoint.SequenceNumberValidator validator = new Checkpoint.SequenceNumberValidator(amazonKinesis, streamName,
                shardId, !validateWithGetIterator);
        String sequenceNumber = "456";

        // Just checking that the false flag for validating against getIterator is honored
        validator.validateSequenceNumber(sequenceNumber);

        verify(amazonKinesis, never()).getShardIterator(any(GetShardIteratorRequest.class));
    }

    @Test
    public void nonNumericValueValidationTest() {
        Checkpoint.SequenceNumberValidator validator = new Checkpoint.SequenceNumberValidator(amazonKinesis, streamName,
                shardId, validateWithGetIterator);

        String[] nonNumericStrings = {null,
                "bogus-sequence-number",
                SentinelCheckpoint.LATEST.toString(),
                SentinelCheckpoint.TRIM_HORIZON.toString(),
                SentinelCheckpoint.AT_TIMESTAMP.toString()};

        Arrays.stream(nonNumericStrings).forEach(sequenceNumber -> {
            try {
                validator.validateSequenceNumber(sequenceNumber);
                fail("Validator should not consider " + sequenceNumber + " a valid sequence number");
            } catch (IllegalArgumentException e) {
                // Do nothing
            }
        });

        verify(amazonKinesis, never()).getShardIterator(any(GetShardIteratorRequest.class));
    }

    @Test
    public final void testIsDigits() {
        // Check things that are all digits
        String[] stringsOfDigits = {"0", "12", "07897803434", "12324456576788"};

        for (String digits : stringsOfDigits) {
            assertTrue("Expected that " + digits + " would be considered a string of digits.",
                    Checkpoint.SequenceNumberValidator.isDigits(digits));
        }
        // Check things that are not all digits
        String[] stringsWithNonDigits = {
                null,
                "",
                "      ", // white spaces
                "6 4",
                "\t45",
                "5242354235234\n",
                "7\n6\n5\n",
                "12s", // last character
                "c07897803434", // first character
                "1232445wef6576788", // interior
                "no-digits",
        };
        for (String notAllDigits : stringsWithNonDigits) {
            assertFalse("Expected that " + notAllDigits + " would not be considered a string of digits.",
                    Checkpoint.SequenceNumberValidator.isDigits(notAllDigits));
        }
    }*/
}
