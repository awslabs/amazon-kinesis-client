package software.amazon.kinesis.coordinator.assignment.packing;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
abstract class SimulatedAnnealing {

    private static final double TEMPERATURE_CUTOFF = 0.01;

    private double temperature = 1;
    private double coolingFactor = 0.95;
    private long timeout = 5000L;

    protected abstract long objective();

    protected abstract void neighbor();

    protected abstract void accept();

    protected abstract void reject();

    public final void anneal() {
        Instant start = Instant.now();
        long objective = objective();

        // iterate until cool or timed out
        for (double t = temperature; t > TEMPERATURE_CUTOFF && elapsed(start) < timeout; t *= coolingFactor) {
            neighbor();

            long newObjective = objective();
            long delta = newObjective - objective;

            // if better, accept; else, use probability formula e^(delta/temperature)
            if (delta > 0 || new Random().nextDouble() < Math.exp(delta / t)) {
                objective = newObjective;
                accept();
            } else {
                reject();
            }
        }
    }

    protected final long elapsed(Instant start) {
        return Duration.between(start, Instant.now()).toMillis();
    }
}
