/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package software.amazon.kinesis.retrieval.fanout;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.fanout.FanOutRecordsPublisher;
import software.amazon.kinesis.retrieval.fanout.experimental.ExperimentalFanOutRecordsPublisher;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

//
// This has to be in the fanout package as it accesses internal classes of the FanOutRecordsPublisher but tests
// a descendant of that class.
//

@RunWith(MockitoJUnitRunner.class)
public class ExperimentalFanOutRecordsPublisherTest {

    private static final String SHARD_ID = "shardId-000000000001";
    private static final String CONSUMER_ARN = "arn:consumer";

    @Mock
    private KinesisAsyncClient kinesisClient;
    @Mock
    private SdkPublisher<SubscribeToShardEventStream> publisher;
    @Mock
    private Subscription subscription;

    @Test
    public void mismatchedShardIdTest() {
        FanOutRecordsPublisher source = new ExperimentalFanOutRecordsPublisher(kinesisClient, SHARD_ID, CONSUMER_ARN);

        ArgumentCaptor<FanOutRecordsPublisher.RecordSubscription> captor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordSubscription.class);
        ArgumentCaptor<FanOutRecordsPublisher.RecordFlow> flowCaptor = ArgumentCaptor
                .forClass(FanOutRecordsPublisher.RecordFlow.class);

        doNothing().when(publisher).subscribe(captor.capture());

        source.start(ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        List<Throwable> errorsHandled = new ArrayList<>();
        List<ProcessRecordsInput> inputsReceived = new ArrayList<>();

        source.subscribe(new Subscriber<ProcessRecordsInput>() {
            Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                subscription.request(1);
            }

            @Override
            public void onNext(ProcessRecordsInput input) {
                inputsReceived.add(input);
            }

            @Override
            public void onError(Throwable t) {
                errorsHandled.add(t);

            }

            @Override
            public void onComplete() {
                fail("OnComplete called when not expected");
            }
        });

        verify(kinesisClient).subscribeToShard(any(SubscribeToShardRequest.class), flowCaptor.capture());
        flowCaptor.getValue().onEventStream(publisher);
        captor.getValue().onSubscribe(subscription);

        List<Record> records = Stream.of(1, 2, 3).map(seq -> FanOutRecordsPublisherTest.makeRecord(seq, seq)).collect(Collectors.toList());

        SubscribeToShardEvent batchEvent = SubscribeToShardEvent.builder().millisBehindLatest(100L).records(records).build();

        captor.getValue().onNext(batchEvent);

        verify(subscription, times(1)).request(1);
        assertThat(inputsReceived.size(), equalTo(0));
        assertThat(errorsHandled.size(), equalTo(1));
        assertThat(errorsHandled.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(errorsHandled.get(0).getMessage(), containsString("Received records destined for different shards"));
    }




}