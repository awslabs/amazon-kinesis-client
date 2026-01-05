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
    }
}
