package software.amazon.kinesis.coordinator.assignment.packing;

import java.util.Arrays;

import org.ojalgo.optimisation.Optimisation;
import software.amazon.kinesis.coordinator.assignment.packing.BinPackingModel.Bin;
import software.amazon.kinesis.coordinator.assignment.packing.BinPackingModel.Item;
import software.amazon.kinesis.coordinator.assignment.packing.BinPackingModel.Metric;
import software.amazon.kinesis.coordinator.assignment.packing.BinPackingModel.Packing;

public class BinPackingSolver {

    public static void main(String[] args) {
        System.out.println("Starting solve...");

        if (2 + 2 == 4) {
            greedy();
            return;
        }

        // Enable DEBUG level logging
        // BasicLogger.debug();

        // generate data (multiple metrics)
        int numItems = 100;
        int maxSize = 1000;
        int numMetrics = 3;
        int itemsPerBin = numItems / 10;
        int numBins = 10;
        double maxWeightedSpread = 0.1;

        long[] sums = new long[numMetrics];
        long[][] values = new long[numItems][numMetrics];

        for (int i = 0; i < numItems; i++) {
            for (int j = 0; j < numMetrics; j++) {
                values[i][j] = (long) (Math.random() * maxSize);
                sums[j] += values[i][j];
            }
        }

        // create items from data
        Item[] items = new Item[values.length];
        for (int i = 0; i < values.length; i++) {
            items[i] = Item.builder().id(i).values(values[i]).build();
        }

        // sort the items (they have ids to track them down later)
        Arrays.sort(items, (a, b) -> Math.toIntExact(b.values[0] - a.values[0]));

        /*
        for (Item item : items) {
            System.out.println(item.id + " " + item.values[0]);
        }
         */

        // create metrics
        Metric[] metrics = new Metric[numMetrics];
        for (int j = 0; j < numMetrics; j++) {
            double capacity = (sums[j] / numBins);
            System.out.println("target capacity for metric " + j + " : " + capacity);
            metrics[j] = Metric.builder().capacity(capacity).build();
        }

        // create a BinPackingIP (provide items and metrics)
        BinPackingIP program = BinPackingIP.builder()
                .items(items)
                .metrics(metrics)
                .trackUnderfill(true)
                // .minimax(true)
                .build();

        /*
        // start from FFD solution for items
        Packing ffd = program.new Packing(true);

        for (Bin bin : ffd.getBins()) {
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
            for (long load : bin.loads) {
                System.out.println(load);
            }
        }
         */

        /*
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
         */

        // use FFD (computed earlier) as base state (can set numBins < maxBins and add binCost to use flexible bins)
        // from testing => flexible bins is relatively expensive, feasible precheck is probably much better usually
        // flexible would be useful if can afford extra bins and solve time for improved least squares (L2) balance
        // program.baseState = ffd;
        // program.numBins = program.maxBins = ffd.numBins();
        // program.binCost = 10L;

        // program.numBins = program.maxBins = ffd.numBins();
        program.numBins = program.maxBins = numBins;

        // penalty of 5L weight per reassignment with maximum of 20 percent of number of items
        // the model MUST use symmetry-breaking constraints when bin range is not fixed to base state numBins
        // program.reassignmentLimit = (int) (numItems * 0.2);
        // program.reassignmentPenalty = 5L;

        /*
        for (int k = 0; k < metrics.length; k++) {
            // try cutting each capacity in half
            metrics[k] = Metric.builder().capacity(metrics[k].capacity * 0.9).build();
        }
         */

        /*
         * We do not need to cut hard capacity constraints to "compress" the bins if we optimize max underfill slack.
         * Instead of minimax objective, set hard constraint on max "spread" (i.e. min slack minus max slack).
         */

        // program.maxWeightedSpread = maxWeightedSpread;

        // add overfill slack, add smoothing for non-slack, and re-compile with objective
        // smoothing > 0 || metrics[k].smoothing > 0 significantly degrades performance because QP solver must be used
        // (possible to use piecewise linear approximation to bypass switching to much more expensive QP)
        // program.trackOverfill = true;
        // program.smoothing = 0.2; // (from testing => do not use; QP solver takes forever)
        // compile without the objective, as it adds symmetry
        program.compile(true);

        // solve the model
        Optimisation.Result finalResult = program.solve();

        System.out.println(finalResult);

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
            for (long load : bin.loads) {
                System.out.println(load);
            }
        }
    }

    private static void greedy() {
        int numItems = 1000;
        int numBins = 100;
        int numMetrics = 2;

        int maxSize = 1000;

        long[] sums = new long[numMetrics];
        long[][] values = new long[numItems][numMetrics];

        for (int i = 0; i < numItems; i++) {
            for (int j = 0; j < numMetrics; j++) {
                values[i][j] = (long) (Math.random() * maxSize);
                sums[j] += values[i][j];
            }
        }

        // create items from data
        Item[] items = new Item[values.length];
        for (int i = 0; i < values.length; i++) {
            items[i] = Item.builder().id(i).values(values[i]).build();
        }

        // create metrics
        Metric[] metrics = new Metric[numMetrics];
        for (int j = 0; j < numMetrics; j++) {
            double capacity = sums[j] / numBins;
            System.out.println("target capacity for metric " + j + " : " + capacity);
            metrics[j] = Metric.builder().capacity(sums[j] / numBins).build();
        }

        // sort the items (they have ids to track them down later)
        // don't sort yet; need to figure out how to use the ids the lp uses to extract/add back to bin packing
        // Arrays.sort(items, (a, b) -> Math.toIntExact(b.values[0] - a.values[0]));

        // create the model (provide items and metrics, and set numBins)
        BinPackingModel model = BinPackingModel.builder()
                .items(items)
                .metrics(metrics)
                .numBins(numBins)
                .build();

        // run FFD with greedy batch assignment LPs
        Packing packing = model.new Packing(numBins);

        // print solution
        for (Bin bin : packing.getBins()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
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
            for (long load : bin.loads) {
                System.out.println(load);
            }
        }
    }
}
