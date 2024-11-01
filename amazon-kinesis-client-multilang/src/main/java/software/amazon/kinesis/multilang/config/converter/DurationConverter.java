package software.amazon.kinesis.multilang.config.converter;

import java.time.Duration;

import org.apache.commons.beanutils.Converter;

/**
 * Converter that converts Duration text representation to a Duration object.
 * Refer to {@code Duration.parse} javadocs for the exact text representation.
 */
public class DurationConverter implements Converter {

    @Override
    public <T> T convert(Class<T> type, Object value) {
        if (value == null) {
            return null;
        }

        if (type != Duration.class) {
            throw new ConversionException("Can only convert to Duration");
        }

        String durationString = value.toString().trim();
        final Duration duration = Duration.parse(durationString);
        if (duration.isNegative()) {
            throw new ConversionException("Negative values are not permitted for duration: " + durationString);
        }

        return type.cast(duration);
    }

    public static class ConversionException extends RuntimeException {
        public ConversionException(String message) {
            super(message);
        }
    }
}
