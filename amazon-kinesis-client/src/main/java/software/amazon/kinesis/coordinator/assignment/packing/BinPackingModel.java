package software.amazon.kinesis.coordinator.assignment.packing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class BinPackingModel {

    protected int numBins;
    protected Packing baseState;
    protected long reassignmentPenalty;
    protected int reassignmentLimit;

    // parallel arrays; values in each item should have same order as metrics they correspond to
    @NonNull
    protected Item[] items;

    @NonNull
    protected Metric[] metrics;

    @RequiredArgsConstructor
    @SuperBuilder
    @Value
    public static class Item {
        protected int id; // set id to index in items array
        protected long[] values; // set length to metrics.length
    }

    @SuperBuilder
    @Value
    public static class Metric {
        protected double weight = 1;
        protected double capacity;
        // defines a linear/quadratic ratio; 0 is pure linear (L1), 1 is square function (L2)
        protected double smoothing = 0;
        protected double average;
    }

    @Getter
    protected class Bin {
        protected Map<Integer, Item> items = new HashMap<>();
        protected long[] loads = new long[metrics.length];

        public void softAdd(Item item) {
            for (int i = 0; i < metrics.length; i++) {
                loads[i] += item.values[i];
            }
            items.put(item.id, item);
        }

        public boolean hardAdd(Item item) {
            for (int i = 0; i < metrics.length; i++) {
                if ((loads[i] += item.values[i]) > metrics[i].capacity) {
                    while (i >= 0) {
                        loads[i] -= item.values[i--];
                    }
                    return false;
                }
            }
            items.put(item.id, item);
            return true;
        }

        /**
         * version of hard add where:
         *      if found fit, return -1
         *      else, return score of how much it was off by
         */
        public double score(Item item) {
            double score = 0.0;
            boolean fits = true;

            for (int k = 0; k < metrics.length; k++) {
                long load = loads[k] + item.values[k];
                long overflow = load - (long) metrics[k].capacity;

                if (overflow > 0) {
                    fits = false;
                    score += metrics[k].weight * overflow;
                }
            }

            if (fits) {
                softAdd(item);
                return -1;
            }
            return score;
        }

        public Item remove(int id) {
            Item item = items.remove(id);

            for (int i = 0; i < metrics.length; i++) {
                loads[i] -= item.values[i];
            }
            return item;
        }
    }

    @Getter
    public class Packing {
        protected List<Bin> bins = new ArrayList<>();
        protected Map<Integer, Bin> itemMap = new HashMap<>();

        protected int numBins;
        protected AssignmentLP lp;
        // potentially use this to map lp ids back to item.id?
        // protected Map<Integer, Integer> batchMap = new HashMap<>();

        public Packing(int numBins) {
            for (int j = 0; j < (this.numBins = numBins); j++) {
                bins.add(new Bin());
            }

            modifiedFirstFit(items);
        }

        public Packing(boolean firstFit) {
            if (firstFit) {
                firstFit();
            }
        }

        public int numBins() {
            return bins.size();
        }

        private void firstFit() {
            for (Item item : items) {
                firstFit(item);
            }
        }

        private void firstFit(Item item) {
            for (Bin bin : bins) {
                if (bin.hardAdd(item)) {
                    itemMap.put(item.id, bin);
                    return;
                }
            }
            Bin bin = new Bin();
            bin.softAdd(item);
            bins.add(bin);
            itemMap.put(item.id, bin);
        }

        private void modifiedFirstFit(Item[] items) {
            for (Item item : items) {
                modifiedFirstFit(item);
            }

            if (lp != null) {
                // execute final incomplete batch
                lp.compile();
                lp.solve();

                // add results to items
                lp.extract(this);
            }
        }

        private void modifiedFirstFit(Item item) {
            lp = lp != null ? lp : new AssignmentLP(items.length, numBins, metrics.length);

            double[] scores = new double[numBins];

            int j = 0;
            for (Bin bin : bins) {
                scores[j] = bin.score(item);

                if (scores[j++] == -1) {
                    itemMap.put(item.id, bin);
                    return;
                }
            }

            // could not find a fit; add to batch
            lp.scores[lp.numItems++] = scores;
            // batchMap.put(lp.numItems++, item.id);

            if (lp.numItems == numBins) {
                // full batch; must execute
                lp.compile();
                lp.solve();

                // add results to items
                lp.extract(this);

                // reset lp
                lp = null;
            }
        }

        public void softAdd(int i, int j) {
            Bin bin = bins.get(j);
            bin.softAdd(items[i]);
            itemMap.put(i, bin);
        }
    }
}
