package software.amazon.kinesis.coordinator.assignment.packing;

import java.math.BigDecimal;

import lombok.experimental.SuperBuilder;
import org.ojalgo.optimisation.Expression;
import org.ojalgo.optimisation.ExpressionsBasedModel;
import org.ojalgo.optimisation.Optimisation;
import org.ojalgo.optimisation.Variable;

@SuperBuilder
class BinPackingIP extends BinPackingModel {

    Packing state;
    Packing baseState;

    int maxBins;
    boolean hasObjective;
    long binCost;
    boolean trackUnderfill;
    boolean trackOverfill;
    double smoothing; // smoothing ratio for any non-slack variables (0 is L1/linear, 1 is L2/quadratic)
    boolean flexibleBins;

    // below fields are set with correct sizes/values during compile(); do not use Builder
    ExpressionsBasedModel model;

    Expression objective;
    Expression reassignmentLimitExpr;
    Expression numBinsExpr;
    Expression[] assignmentExpr;
    Expression[] reassignmentExpr;
    Expression[] nonEmptyExpr;
    Expression[] ordering;
    Expression[][] capacity;

    Variable[][] assignment;
    Variable[][] underfill;
    Variable[][] overfill;
    Variable[] reassignment;
    Variable[] nonEmpty;
    Variable numBinsUsed;

    Optimisation.Result result;

    public Optimisation.Result solve() {
        return (result = model.minimise());
    }

    public void compile(boolean skipObjective) {
        model = new ExpressionsBasedModel();

        assignmentVariables();
        assignmentConstraints();

        if (baseState != null) {
            reassignmentVariables();

            if (reassignmentLimit != 0) {
                reassignmentLimit(reassignmentLimit);
            }
        }

        if (trackUnderfill) {
            trackUnderfill();
        }
        if (trackOverfill) {
            trackOverfill();
        }

        capacityConstraints();

        if (numBins < maxBins) {
            System.out.println("Using flexible bins...");
            flexibleBins();
        }

        // orderingConstraints();

        if (!skipObjective) {
            objective();
        }
    }

    private void assignmentVariables() {
        assignment = new Variable[items.length][maxBins];

        for (int i = 0; i < items.length; i++) {
            for (int j = 0; j < maxBins; j++) {
                assignment[i][j] =
                        model.addVariable("assignment_" + i + "_" + j).binary();
            }
        }
    }

    private void assignmentConstraints() {
        assignmentExpr = new Expression[items.length];

        for (int i = 0; i < items.length; i++) {
            assignmentExpr[i] = model.addExpression("assignment_" + i);
            assignmentExpr[i].level(BigDecimal.ONE);

            for (int j = 0; j < maxBins; j++) {
                assignmentExpr[i].set(assignment[i][j], BigDecimal.ONE);
            }
        }
    }

    private void trackUnderfill() {
        underfill = new Variable[maxBins][metrics.length];

        for (int j = 0; j < maxBins; j++) {
            for (int k = 0; k < metrics.length; k++) {
                underfill[j][k] = model.addVariable("underfill" + k + "_" + j).lower(BigDecimal.ZERO);
            }
        }
    }

    private void trackOverfill() {
        overfill = new Variable[maxBins][metrics.length];

        for (int j = 0; j < maxBins; j++) {
            for (int k = 0; k < metrics.length; k++) {
                overfill[j][k] = model.addVariable("overfill" + k + "_" + j).lower(BigDecimal.ZERO);
            }
        }
    }

    private void capacityConstraints() {
        capacity = new Expression[maxBins][metrics.length];

        for (int j = 0; j < maxBins; j++) {
            capacity[j] = new Expression[metrics.length];

            for (int k = 0; k < metrics.length; k++) {
                capacity[j][k] = model.addExpression("capacity" + k + "_" + j);
                capacity[j][k].level(metrics[k].capacity);

                for (int i = 0; i < items.length; i++) {
                    capacity[j][k].set(assignment[i][j], items[i].values[k]);
                }
            }
        }

        if (trackUnderfill) {
            for (int j = 0; j < maxBins; j++) {
                for (int k = 0; k < metrics.length; k++) {
                    capacity[j][k].set(underfill[j][k], BigDecimal.ONE);
                }
            }
        }
        if (trackOverfill) {
            for (int j = 0; j < maxBins; j++) {
                for (int k = 0; k < metrics.length; k++) {
                    capacity[j][k].set(overfill[j][k], BigDecimal.ONE.negate());
                }
            }
        }
    }

    private void nonEmptyConstraints() {
        nonEmpty = new Variable[maxBins];
        nonEmptyExpr = new Expression[maxBins];

        for (int b = 0; b < maxBins; b++) {
            nonEmpty[b] = model.addVariable("nonEmpty_" + b).binary();
            nonEmptyExpr[b] = model.addExpression("nonEmptyExpr_" + b);

            for (int i = 0; i < items.length; i++) {
                nonEmptyExpr[b].set(assignment[i][b], BigDecimal.ONE);
            }

            // big M method: bin.size <= items.length
            nonEmptyExpr[b].set(nonEmpty[b], BigDecimal.valueOf(-items.length));
            nonEmptyExpr[b].upper(BigDecimal.ZERO);
        }
    }

    private void orderingConstraints() {
        ordering = new Expression[maxBins];

        // symmetry breaking: cannot use bin b unless bin b-1 is used
        for (int b = 1; b < maxBins; b++) {
            ordering[b] = model.addExpression("ordering_" + b);
            ordering[b].set(nonEmpty[b], BigDecimal.ONE);
            ordering[b].set(nonEmpty[b - 1], BigDecimal.ONE.negate());
            ordering[b].upper(BigDecimal.ZERO);
        }
    }

    private void flexibleBins() {
        flexibleBins = true;

        nonEmptyConstraints();
        orderingConstraints();
        numBinsExpression();
    }

    private void numBinsExpression() {
        numBinsUsed = model.addVariable("numBins").integer();
        numBinsUsed.lower(BigDecimal.ONE);
        numBinsUsed.upper(maxBins);

        numBinsExpr = model.addExpression("numBinsExpr");
        numBinsExpr.set(numBinsUsed, BigDecimal.ONE);
        numBinsExpr.level(BigDecimal.ZERO);

        for (int b = 0; b < maxBins; b++) {
            numBinsExpr.set(nonEmpty[b], BigDecimal.ONE.negate());
        }
    }

    private void reassignmentVariables() {
        reassignment = new Variable[items.length];
        reassignmentExpr = new Expression[items.length];

        for (int i = 0; i < items.length; i++) {
            reassignment[i] = model.addVariable("reassignment_" + i).binary();
        }

        int j = 0;
        for (Bin bin : baseState.bins) {
            for (int i : bin.items.keySet()) {
                reassignmentExpr[i] = model.addExpression("reassignmentExpr_" + i);
                reassignmentExpr[i].set(assignment[i][j], BigDecimal.ONE);
                reassignmentExpr[i].set(reassignment[i], BigDecimal.ONE);
                reassignmentExpr[i].lower(BigDecimal.ONE);
            }
            j++;
        }
    }

    private void reassignmentLimit(int limit) {
        reassignmentLimitExpr = model.addExpression("reassignmentLimit");
        reassignmentLimitExpr.upper(limit);

        for (int i = 0; i < items.length; i++) {
            reassignmentLimitExpr.set(reassignment[i], BigDecimal.ONE);
        }
    }

    private void objective() {
        objective = model.addExpression("minimize_slack");
        objective.weight(BigDecimal.ONE);

        if (trackUnderfill) {
            for (int j = 0; j < maxBins; j++) {
                for (int k = 0; k < metrics.length; k++) {
                    objectiveTerm(underfill[j][k], metrics[k].weight, metrics[k].smoothing);
                }
            }
        }
        if (trackOverfill) {
            for (int j = 0; j < maxBins; j++) {
                for (int k = 0; k < metrics.length; k++) {
                    objectiveTerm(overfill[j][k], metrics[k].weight, metrics[k].smoothing);
                }
            }
        }
        if (reassignmentPenalty > 0) {
            for (int i = 0; i < reassignment.length; i++) {
                objectiveTerm(reassignment[i], reassignmentPenalty, smoothing);
            }
        }
        if (flexibleBins && binCost > 0) {
            for (int b = 0; b < maxBins; b++) {
                objectiveTerm(nonEmpty[b], binCost, smoothing);
            }
        }
    }

    private void objectiveTerm(Variable v, double weight, double smoothing) {
        double linear = 1 - smoothing;
        double quadratic = smoothing;

        if (linear > 0) {
            System.out.println("found linear term!");
            objective.set(v, weight * linear);
        }
        if (quadratic > 0) {
            System.out.println("found quadratic term!");
            objective.set(v, v, weight * quadratic);
        }
    }

    /*
    public int getNumBinsUsed() {
        int left = 0;
        int right = Math.max(0, maxBins - 1);
        int lastUsed = -1;

        // binary search for the last bin with nonEmpty[b] != null && nonEmpty[b] == 1
        while (left <= right) {
            int mid = left + (right - left) / 2;
            BigDecimal value = nonEmpty[mid].getValue();

            if (value != null && value.intValue() == 1) {
                lastUsed = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return lastUsed + 1;
    }
     */

    public int getNumBinsUsed() {
        return flexibleBins ? numBinsUsed.getValue().intValue() : numBins /* must equal maxBins */;
    }

    public Packing extract() {
        Packing solution = new Packing(false);

        int numBinsUsed = getNumBinsUsed();

        System.out.println("num bins used: " + numBinsUsed);

        for (int j = 0; j < numBinsUsed; j++) {
            Bin bin = new Bin();
            solution.bins.add(bin);

            for (int i = 0; i < items.length; i++) {
                if (assignment[i][j].getValue().compareTo(BigDecimal.valueOf(0.5)) > 0) { // compare with tolerance
                    bin.softAdd(items[i]);
                    solution.itemMap.put(i, bin);
                }
            }
        }
        return solution;
    }
}
