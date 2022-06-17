package software.amazon.kinesis.multilang.messages;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@Getter
@Setter
@Slf4j
public class LogMessage extends Message {
    /**
     * The name used for the action field in {@link Message}.
     */
    public static final String ACTION = "log";

    /**
     * The shard id that this processor is getting initialized for.
     */
    private String message;
    private String logLevel = "info";


    public LogMessage() {
        this.message = "initialized";
    }

    /**
     * Default constructor.
     */
    public LogMessage(String message) {
        this.message = message;
        log.info("Client logging: " + this.message);
    }

    public Function<String, Boolean> getLogger() {
        switch (logLevel) {
            case "debug": {
                return (m) -> {
                    log.debug(m);
                    return true;
                };
            }
            case "warn":
                return (m) -> {
                    log.warn(m);
                    return true;
                };
            case "error":
                return (m) -> {
                    log.error(m);
                    return true;
                };
            default:
                return (m) -> {
                    log.info(m);
                    return true;
                };
        }
    }
}
