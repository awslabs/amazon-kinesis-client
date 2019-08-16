/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.lifecycle;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RecordsRetrieved;
import software.amazon.kinesis.retrieval.RecordsDeliveryAck;

/**
 * Subscriber that notifies its publisher on receipt of the onNext event.
 */
public interface NotifyingSubscriber extends Subscriber<RecordsRetrieved> {

    /**
     * Return the actual subscriber to which the events needs to be delegated.
     * @return Subscriber<T> to be delegated
     */
    Subscriber<RecordsRetrieved> getDelegateSubscriber();

    /**
     * Return the publisher to be notified
     * @return RecordsPublisher to be notified.
     */
    RecordsPublisher getRecordsPublisher();

    /**
     * Construct RecordsDeliveryAck object from the incoming data and return it
     * @param recordsRetrieved for which we need the ack.
     * @return getRecordsDeliveryAck
     */
    RecordsDeliveryAck getRecordsDeliveryAck(RecordsRetrieved recordsRetrieved);

    @Override
    default void onSubscribe(Subscription subscription) {
        getDelegateSubscriber().onSubscribe(subscription);
    }

    @Override
    default void onNext(RecordsRetrieved recordsRetrieved) {
        getRecordsPublisher().notify(getRecordsDeliveryAck(recordsRetrieved));
        getDelegateSubscriber().onNext(recordsRetrieved);
    }

    @Override
    default void onError(Throwable throwable) {
        getDelegateSubscriber().onError(throwable);
    }

    @Override
    default void onComplete() {
        getDelegateSubscriber().onComplete();
    }
}
