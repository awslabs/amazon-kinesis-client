package software.amazon.kinesis.coordinator.assignment.packing;

import org.ojalgo.optimisation.Optimisation;
import software.amazon.kinesis.coordinator.assignment.packing.BinPackingModel.Bin;
import software.amazon.kinesis.coordinator.assignment.packing.BinPackingModel.Item;
import software.amazon.kinesis.coordinator.assignment.packing.BinPackingModel.Metric;
import software.amazon.kinesis.coordinator.assignment.packing.BinPackingModel.Packing;

public class BinPackingSolver {

    public static void main(String[] args) {
        System.out.println("Starting solve...");

        // generate data (single metric)
        int numItems = 1000;
        int maxSize = 1000;
        int itemsPerBin = numItems / 10;

        long sum = 0L;
        long[] values = new long[numItems];

        for (int i = 0; i < numItems; i++) {
            values[i] = (long) (Math.random() * maxSize);
            sum += values[i];
        }

        double average = sum / numItems;
        double capacity = average * itemsPerBin;

        // create items from data
        Item[] items = new Item[values.length];
        for (int i = 0; i < values.length; i++) {
            items[i] = Item.builder().id(i).values(new long[] {values[i]}).build();
        }

        // create single metric
        Metric[] metrics = new Metric[1];
        metrics[0] = Metric.builder().capacity(capacity).build();

        // create a BinPackingIP (provide items and metrics)
        BinPackingIP program = BinPackingIP.builder()
                .items(items)
                .metrics(metrics)
                .trackUnderfill(true)
                .build();

        // start from FFD solution for items
        Packing ffd = program.new Packing(true);

        // fix number of bins same as FFD (target extra low so feasibility check fails and we have to scale up)
        program.numBins = program.maxBins = ffd.numBins() / 2;

        // compile the program without the objective
        program.compile(true);

        // check feasibility (trackUnderfill=true && trackOverfill=false is required before compile() for feasibility
        // checks)
        Optimisation.Result result = program.solve();
        while (result.getState() != Optimisation.State.FEASIBLE && result.getState() != Optimisation.State.OPTIMAL) {
            System.out.println("Not feasible for number of bins!");

            // not feasible! doubling number of bins...
            program.numBins = program.maxBins *= 2;

            // re-compile and re-solve with updated/relaxed allowed bins
            program.compile(true);
            result = program.solve();
        }

        // use FFD (computed earlier) as base state
        program.baseState = ffd;

        // penalty of 3L weight per reassignment with maximum of 5 percent of number of items
        program.reassignmentLimit = (int) (numItems * 0.05);
        program.reassignmentPenalty = 3L;

        // add overfill slack, add smoothing for non-slack, and re-compile with objective
        // be absolutely sure the model adds symmetry-breaking constraints given the number of bins allowed is not the
        // exact same as FFD, as any bin number not in the FFD solution will be equally good to place an item into
        program.trackOverfill = true;
        program.smoothing = 0.2;
        program.compile(false);

        // solve the model
        Optimisation.Result finalResult = program.solve();

        // extract the solution (known to be feasible)
        Packing solution = program.extract();

        // print the solution
        System.out.println("Solution... \n");

        for (Bin bin : solution.getBins()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[ ");
            for (int i : bin.getItems().keySet()) {
                sb.append("( ");
                for (long v : items[i].values) {
                    sb.append(v + " ");
                }
                sb.append(")");
            }
            sb.append("] => ");
            sb.append("( ");
            for (long load : bin.loads) {
                sb.append(load + " ");
            }
            sb.append(")");
            System.out.println(sb);
        }
    }
}
