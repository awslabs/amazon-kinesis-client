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

        System.out.println("target capacity... " + capacity);

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
        program.numBins = program.maxBins = (int) Math.ceil(ffd.numBins() / 2);

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

        // use FFD (computed earlier) as base state (can set numBins < maxBins and add binCost to use flexible bins)
        // from testing => flexible bins is relatively expensive, feasible precheck is probably much better usually
        // flexible would be useful if can afford extra bins and solve time for improved least squares (L2) balance
        program.baseState = ffd;
        program.numBins = program.maxBins = ffd.numBins();
        // program.binCost = 10L;

        // penalty of 5L weight per reassignment with maximum of 10 percent of number of items
        // the model MUST use symmetry-breaking constraints when bin range is not fixed to base state numBins
        program.reassignmentLimit = (int) (numItems * 0.0);
        program.reassignmentPenalty = 10000L;

        // add overfill slack, add smoothing for non-slack, and re-compile with objective
        // smoothing > 0 || metrics[k].smoothing > 0 significantly degrades performance because QP solver must be used
        // (possible to use piecewise linear approximation to bypass switching to much more expensive QP)
        program.trackOverfill = true; // (no need to track overfill if using FFD / hard-capacity base state)
        // tracking overfill expands search space as solver explores overpacking the bins when underpacking is just
        // as good from the objective's perspective; can asymmetrically prioritize, or just drop one side
        // program.smoothing = 0.2; (from testing => do not use; QP solver takes forever)
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
