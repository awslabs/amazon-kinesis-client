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
    boolean minimax;
    double maxWeightedSum;
    double maxWeightedSpread;

    // below fields are set with correct sizes/values during compile(); do not use Builder
    ExpressionsBasedModel model;

    Expression objective;
    Expression reassignmentLimitExpr;
    Expression numBinsExpr;
    Expression maxSpreadExpr;
    Expression[] assignmentExpr;
    Expression[] reassignmentExpr;
    Expression[] nonEmptyExpr;
    Expression[] ordering;
    Expression[] weightedSlackExpr;
    Expression[] maxSlackExpr;
    Expression[] minSlackExpr;
    Expression[][] capacity;

    Variable[][] assignment;
    Variable[][] underfill;
    Variable[][] overfill;
    Variable[] reassignment;
    Variable[] nonEmpty;
    Variable[] weightedSlack;
    Variable[] orderingViolation;
    Variable numBinsUsed;
    Variable maxSlack;
    Variable minSlack;
    Variable spread;

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

        weightedSums();
        // minimax();

        // full ordering of bins by weighted slack, where first bin is "worst"
        // calculate big M as weighted sum if all items were packed into the same bin
        // orderingConstraints(weightedSlack, maxWeightedSum());

        // use partial ordering instead; increases chance of tight top-level LP relaxation to 1/n instead of 1/n!
        // partialOrderingConstraints(weightedSlack);

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

    private void partialOrderingConstraints(Variable[] binVars) {
        ordering = new Expression[maxBins];

        // symmetry breaking: binVars[0] >= binVars[b]
        for (int b = 1; b < maxBins; b++) {
            ordering[b] = model.addExpression("ordering_" + b);
            ordering[b].set(binVars[b], BigDecimal.ONE);
            ordering[b].set(binVars[0], BigDecimal.ONE.negate());
            ordering[b].upper(BigDecimal.ZERO);
        }
    }

    private void orderingConstraints(Variable[] binVars, double bigM) {
        ordering = new Expression[maxBins];
        orderingViolation = new Variable[maxBins];

        // avoid null pointer as bin 0 can't violate ordering
        orderingViolation[0] = model.addVariable("orderingViolationDummy").level(0);

        // symmetry breaking: binVars[b-1] >= binVars[b]
        for (int b = 1; b < maxBins; b++) {
            ordering[b] = model.addExpression("ordering_" + b);
            ordering[b].set(binVars[b], BigDecimal.ONE);
            ordering[b].set(binVars[b - 1], BigDecimal.ONE.negate());
            ordering[b].upper(BigDecimal.ZERO);

            // soften ordering constraints so solver does not get "stuck" (i.e. branching but weak bounding)
            // otherwise fractional values from LP relaxation can conflict with preferred ordering
            // unless solver happens to "pick" the correct bins to be small or big on its first try
            // which only happens with probability 1/n! where n is maxBins; all else, solver will hang!
            // remember to add these to the objective
            if (bigM > 0) {
                orderingViolation[b] =
                        model.addVariable("orderingViolation_" + b).lower(BigDecimal.ZERO);
                ordering[b].set(orderingViolation[b], BigDecimal.valueOf(-bigM));
            }
        }
    }

    private void flexibleBins() {
        flexibleBins = true;

        nonEmptyConstraints();
        numBinsExpression();

        // orderingConstraints(nonEmpty) // need some source of asymmetry in bin ordering, could be bin usage
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
        reassignmentLimitExpr.upper(BigDecimal.valueOf(limit));

        for (int i = 0; i < items.length; i++) {
            reassignmentLimitExpr.set(reassignment[i], BigDecimal.ONE);
        }
    }

    private void weightedSums() {
        weightedSlack = new Variable[maxBins];
        weightedSlackExpr = new Expression[maxBins];

        for (int j = 0; j < maxBins; j++) {
            weightedSlack[j] = model.addVariable("weightedSlack_" + j).lower(BigDecimal.ZERO);

            weightedSlackExpr[j] = model.addExpression("weightedSlackExpr_" + j);
            weightedSlackExpr[j].level(BigDecimal.ZERO);
            weightedSlackExpr[j].set(weightedSlack[j], BigDecimal.ONE);

            if (trackUnderfill) {
                for (int k = 0; k < metrics.length; k++) {
                    weightedTerm(weightedSlackExpr[j], underfill[j][k], -metrics[k].weight, metrics[k].smoothing);
                }
            }
            if (trackOverfill) {
                for (int k = 0; k < metrics.length; k++) {
                    weightedTerm(weightedSlackExpr[j], overfill[j][k], -metrics[k].weight, metrics[k].smoothing);
                }
            }
        }
    }

    private void minimax() {
        maxSlack = model.addVariable("maxSlack").lower(BigDecimal.ZERO);
        minSlack = model.addVariable("minSlack").lower(BigDecimal.ZERO);

        for (int b = 0; b < numBins; b++) {
            // maxSlack >= weightedSlack[b]
            Expression maxConstraint = model.addExpression("maxSlackExpr_" + b);
            maxConstraint.set(maxSlack, BigDecimal.ONE);
            maxConstraint.set(weightedSlack[b], BigDecimal.ONE.negate());
            maxConstraint.lower(BigDecimal.ZERO);

            // minSlack <= weightedSlack[b]
            Expression minConstraint = model.addExpression("minSlackExpr_" + b);
            minConstraint.set(minSlack, BigDecimal.ONE);
            minConstraint.set(weightedSlack[b], BigDecimal.ONE.negate());
            minConstraint.upper(BigDecimal.ZERO);
        }

        spread = model.addVariable("spread").lower(BigDecimal.ZERO);
        maxSpreadExpr = model.addExpression("maxSpreadExpr");

        // hard constraint (weighted): maxSlack <= (1 + maxSpread) * minSlack
        // example: maxSpread = 0.03 means maxSlack cannot be more than 3% greater than minSlack
        maxSpreadExpr.set(maxSlack, BigDecimal.ONE.negate());
        maxSpreadExpr.set(minSlack, BigDecimal.valueOf(1.0 + maxWeightedSpread));
        maxSpreadExpr.lower(BigDecimal.ZERO);
    }

    private double maxWeightedSum() {
        if (maxWeightedSum > 0) {
            return maxWeightedSum;
        }

        double[] sums = new double[metrics.length];
        double sum = 0;

        for (int i = 0; i < items.length; i++) {
            for (int k = 0; k < metrics.length; k++) {
                sums[k] += items[i].values[k];
            }
        }

        // use same formula as weightedTerm() method
        for (int k = 0; k < sums.length; k++) {
            double linear = 1 - metrics[k].smoothing;
            double quadratic = metrics[k].smoothing;

            sum += sums[k] * linear;
            sum += Math.pow(sums[k], 2) * quadratic;
        }

        return (maxWeightedSum = sum);
    }

    private void objective() {
        objective = model.addExpression("minimize_slack");
        objective.weight(BigDecimal.ONE);

        /*
        // "nudge" the bins to be ordered by weighted slack to break the symmetry at the LP relaxation level
        for (int j = 0; j < maxBins; j++) {
            weightedTerm(objective, weightedSlack[j], 0.1 * j, 0);
        }
         */

        if (minimax) {
            // max of weighted slacks dominates other objectives like ordering violations
            weightedTerm(objective, weightedSlack[0], maxWeightedSum, smoothing);

            /*
            // ordering violation variables are <= bigM = maxWeightedSum
            for (int j = 0; j < maxBins; j++) {
                weightedTerm(objective, orderingViolation[j], 1, 0);
            }
             */
        } else {
            for (int j = 0; j < maxBins; j++) {
                weightedTerm(objective, weightedSlack[j], 1, 0);
            }
        }
        /*
        if (trackUnderfill) {
            for (int j = 0; j < maxBins; j++) {
                for (int k = 0; k < metrics.length; k++) {
                    weightedTerm(objective, underfill[j][k], metrics[k].weight, metrics[k].smoothing);
                }
            }
        }
        if (trackOverfill) {
            for (int j = 0; j < maxBins; j++) {
                for (int k = 0; k < metrics.length; k++) {
                    weightedTerm(objective, overfill[j][k], metrics[k].weight, metrics[k].smoothing);
                }
            }
        }
        */
        if (reassignmentPenalty > 0) {
            for (int i = 0; i < reassignment.length; i++) {
                weightedTerm(objective, reassignment[i], reassignmentPenalty, smoothing);
            }
        }
        if (flexibleBins && binCost > 0) {
            for (int b = 0; b < maxBins; b++) {
                weightedTerm(objective, nonEmpty[b], binCost, smoothing);
            }
        }
    }

    private void weightedTerm(Expression expr, Variable v, double weight, double smoothing) {
        double linear = 1 - smoothing;
        double quadratic = smoothing;

        if (linear > 0) {
            expr.set(v, BigDecimal.valueOf(weight * linear));
        }
        if (quadratic > 0) {
            expr.set(v, v, BigDecimal.valueOf(weight * quadratic));
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

        for (int j = 0; j < getNumBinsUsed(); j++) {
            Bin bin = new Bin();
            solution.bins.add(bin);

            for (int i = 0; i < items.length; i++) {
                // System.out.println("assignment " + i + "_" + j + " : " + assignment[i][j].getValue());

                if (assignment[i][j].getValue().compareTo(BigDecimal.valueOf(0.5)) > 0) { // compare with tolerance
                    bin.softAdd(items[i]);
                    solution.itemMap.put(i, bin);
                }
            }
        }
        return solution;
    }
}
