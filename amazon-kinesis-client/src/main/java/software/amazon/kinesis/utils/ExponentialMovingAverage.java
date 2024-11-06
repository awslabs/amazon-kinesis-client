/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
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

package software.amazon.kinesis.utils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Uses the formula mentioned below for simple ExponentialMovingAverage
 * <a href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average"/>
 *
 * Values of alpha close to 1 have less of a smoothing effect and give greater weight to recent changes in the data,
 * while values of alpha closer to 0 have a greater smoothing effect and are less responsive to recent changes.
 */
@RequiredArgsConstructor
@KinesisClientInternalApi
public class ExponentialMovingAverage {

    private final double alpha;

    @Getter
    private double value;

    private boolean initialized = false;

    public void add(final double newValue) {
        if (!initialized) {
            this.value = newValue;
            initialized = true;
        } else {
            this.value = alpha * newValue + (1 - alpha) * this.value;
        }
    }
}
