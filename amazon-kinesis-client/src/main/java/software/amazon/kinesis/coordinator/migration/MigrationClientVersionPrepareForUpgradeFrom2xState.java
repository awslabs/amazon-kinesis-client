package software.amazon.kinesis.coordinator.migration;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.coordinator.DynamicMigrationComponentsInitializer;

@Slf4j
@KinesisClientInternalApi
@RequiredArgsConstructor
@ThreadSafe
public class MigrationClientVersionPrepareForUpgradeFrom2xState implements MigrationClientVersionState {

    private final MigrationStateMachine stateMachine;
    private final ScheduledExecutorService stateMachineThreadPool;
    private final DynamicMigrationComponentsInitializer initializer;
    private final Random random;

    private boolean entered;
    private boolean left;

    @Override
    public ClientVersion clientVersion() {
        return ClientVersion.CLIENT_VERSION_PREPARE_TO_UPGRADE_FROM_2X;
    }

    @Override
    public synchronized void enter(final ClientVersion fromClientVersion) {
        if (!entered) {
            log.info("Entering {} from {}", this, fromClientVersion);
            initializer.initializeClientVersionForPrepareForUpgradeFrom2x(fromClientVersion);
            entered = true;
        } else {
            log.info("Not entering {}", left ? "already exited state" : "already entered state");
        }
    }

    @Override
    public synchronized void leave() {
        if (entered && !left) {
            log.info("Leaving {}", this);
            entered = false;
            left = true;
        } else {
            log.info("Cannot leave {}", entered ? "already exited state" : "because state is not active");
        }
    }
}
