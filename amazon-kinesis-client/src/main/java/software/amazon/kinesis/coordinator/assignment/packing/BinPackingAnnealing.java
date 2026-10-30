package software.amazon.kinesis.coordinator.assignment.packing;

public final class BinPackingAnnealing extends SimulatedAnnealing {

    @Override
    protected long objective() {
        return 0;
    }

    @Override
    protected void neighbor() {}

    @Override
    protected void accept() {}

    @Override
    protected void reject() {}
}
