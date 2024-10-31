package software.amazon.kinesis.utils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Uses the formula mentioned below for simple ExponentialMovingAverage
 * <a href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average"/>
 *
 * Values of alpha close to 1 have less of a smoothing effect and give greater weight to recent changes in the data,
 * while values of alpha closer to 0 have a greater smoothing effect and are less responsive to recent changes.
 */
@RequiredArgsConstructor
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
