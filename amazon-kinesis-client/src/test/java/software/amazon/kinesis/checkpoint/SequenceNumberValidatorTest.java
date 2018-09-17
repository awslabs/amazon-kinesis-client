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

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class SequenceNumberValidatorTest {

    private SequenceNumberValidator validator;

    @Before
    public void begin() {
        validator = new SequenceNumberValidator();
    }


    @Test
    public void matchingSequenceNumberTest() {
        String sequenceNumber = "49587497311274533994574834252742144236107130636007899138";
        String expectedShardId = "shardId-000000000000";

        Optional<Integer> version = validator.versionFor(sequenceNumber);
        assertThat(version, equalTo(Optional.of(2)));

        Optional<String> shardId = validator.shardIdFor(sequenceNumber);
        assertThat(shardId, equalTo(Optional.of(expectedShardId)));

        assertThat(validator.validateSequenceNumberForShard(sequenceNumber, expectedShardId), equalTo(Optional.of(true)));
    }

    @Test
    public void shardMismatchTest() {
        String sequenceNumber = "49585389983312162443796657944872008114154899568972529698";
        String invalidShardId = "shardId-000000000001";

        Optional<Integer> version = validator.versionFor(sequenceNumber);
        assertThat(version, equalTo(Optional.of(2)));

        Optional<String> shardId = validator.shardIdFor(sequenceNumber);
        assertThat(shardId, not(equalTo(invalidShardId)));

        assertThat(validator.validateSequenceNumberForShard(sequenceNumber, invalidShardId), equalTo(Optional.of(false)));
    }

    @Test
    public void versionMismatchTest() {
        String sequenceNumber = "74107425965128755728308386687147091174006956590945533954";
        String expectedShardId = "shardId-000000000000";

        Optional<Integer> version = validator.versionFor(sequenceNumber);
        assertThat(version, equalTo(Optional.empty()));

        Optional<String> shardId = validator.shardIdFor(sequenceNumber);
        assertThat(shardId, equalTo(Optional.empty()));

        assertThat(validator.validateSequenceNumberForShard(sequenceNumber, expectedShardId), equalTo(Optional.empty()));
    }

    @Test
    public void sequenceNumberToShortTest() {
        String sequenceNumber = "4958538998331216244379665794487200811415489956897252969";
        String expectedShardId = "shardId-000000000000";

        assertThat(validator.versionFor(sequenceNumber), equalTo(Optional.empty()));
        assertThat(validator.shardIdFor(sequenceNumber), equalTo(Optional.empty()));

        assertThat(validator.validateSequenceNumberForShard(sequenceNumber, expectedShardId), equalTo(Optional.empty()));
    }

    @Test
    public void sequenceNumberToLongTest() {
        String sequenceNumber = "495874973112745339945748342527421442361071306360078991381";
        String expectedShardId = "shardId-000000000000";

        assertThat(validator.versionFor(sequenceNumber), equalTo(Optional.empty()));
        assertThat(validator.shardIdFor(sequenceNumber), equalTo(Optional.empty()));

        assertThat(validator.validateSequenceNumberForShard(sequenceNumber, expectedShardId), equalTo(Optional.empty()));
    }


}
