package software.amazon.kinesis.utils;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

public class Statistics {

    /**
     * Calculates the simple mean of the given values
     * @param values list of values (double)
     * @return mean of the given values, if the {@param values} is empty then returns 0;
     */
    public static double calculateSimpleMean(final List<Double> values) {
        if (values.isEmpty()) {
            return 0D;
        }
        double sum = 0.0;
        for (final double i : values) {
            sum += i;
        }
        return sum / values.size();
    }

    /**
     * For the given values find the standard deviation (SD).
     * For details of SD calculation ref : <a href="https://en.wikipedia.org/wiki/Standard_deviation"/>
     * @param values list of values (double)
     * @return Map.Entry of mean to standard deviation for {@param values}, if {@param values} is empty then return
     *          Map.Entry with 0 as mean and 0 as SD.
     */
    public static Map.Entry<Double, Double> calculateStandardDeviationAndMean(final List<Double> values) {
        if (values.isEmpty()) {
            return new AbstractMap.SimpleEntry<>(0D, 0D);
        }
        final double mean = calculateSimpleMean(values);
        // calculate the standard deviation
        double standardDeviation = 0.0;
        for (final double num : values) {
            standardDeviation += Math.pow(num - mean, 2);
        }
        return new AbstractMap.SimpleEntry<>(mean, Math.sqrt(standardDeviation / values.size()));
    }
}
