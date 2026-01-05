package software.amazon.kinesis.coordinator.assignment.packing;

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

    // below fields are set with correct sizes/values during compile(); do not use Builder
    ExpressionsBasedModel model;

    Expression objective;
    Expression reassignmentLimitExpr;
    Expression[] assignmentExpr;
    Expression[] reassignmentExpr;
    Expression[][] capacity;

    Variable[][] assignment;
    Variable[][] underfill;
    Variable[][] overfill;
    Variable[] reassignment;
    Variable[] empty;

    public Optimisation.Result solve() {
        return model.minimise();
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
            flexibleBins();
        }

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
            assignmentExpr[i].level(1);

            for (int j = 0; j < maxBins; j++) {
                assignmentExpr[i].set(assignment[i][j], 1);
            }
        }
    }

    private void trackUnderfill() {
        underfill = new Variable[maxBins][metrics.length];

        for (int j = 0; j < maxBins; j++) {
            for (int k = 0; k < metrics.length; k++) {
                underfill[j][k] = model.addVariable("underfill" + k + "_" + j).lower(0);
            }
        }
    }

    private void trackOverfill() {
        overfill = new Variable[maxBins][metrics.length];

        for (int j = 0; j < maxBins; j++) {
            for (int k = 0; k < metrics.length; k++) {
                overfill[j][k] = model.addVariable("overfill" + k + "_" + j).lower(0);
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
                    capacity[j][k].set(underfill[j][k], 1);
                }
            }
        }
        if (trackOverfill) {
            for (int j = 0; j < maxBins; j++) {
                for (int k = 0; k < metrics.length; k++) {
                    capacity[j][k].set(overfill[j][k], -1);
                }
            }
        }
    }

    private void flexibleBins() {}

    private void reassignmentVariables() {
        reassignment = new Variable[items.length];
        reassignmentExpr = new Expression[items.length];

        for (int i = 0; i < items.length; i++) {
            reassignment[i] = model.addVariable("reassignment_" + i).binary();
        }

        int j = 0;
        for (Bin bin : baseState.bins) {
            for (int i : bin.items.keySet()) {
                reassignmentExpr[i] = model.addExpression("reassign_" + i);
                reassignmentExpr[i].set(assignment[i][j], 1);
                reassignmentExpr[i].set(reassignment[i], 1);
                reassignmentExpr[i].lower(1);
            }
            j++;
        }
    }

    private void reassignmentLimit(int limit) {
        reassignmentLimitExpr = model.addExpression("max_reassignments");

        for (int i = 0; i < items.length; i++) {
            reassignmentLimitExpr.set(reassignment[i], 1);
        }
        reassignmentLimitExpr.upper(limit);
    }

    private void objective() {
        objective = model.addExpression("minimize_slack");
        objective.weight(1);

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
                    objectiveTerm(overfill[j][k], -metrics[k].weight, metrics[k].smoothing);
                }
            }
        }
        if (reassignmentPenalty > 0) {
            for (int i = 0; i < reassignment.length; i++) {
                objectiveTerm(reassignment[i], reassignmentPenalty, smoothing);
            }
        }
    }

    private void objectiveTerm(Variable v, double weight, double smoothing) {
        double linear = 1 - smoothing;
        double quadratic = smoothing;

        if (linear > 0) {
            objective.set(v, weight * linear);
        }
        if (quadratic > 0) {
            objective.set(v, v, weight * quadratic);
        }
    }

    public Packing extract() {
        Packing solution = new Packing(false);

        for (int j = 0; j < numBins; j++) {
            Bin bin = new Bin();
            solution.bins.add(bin);

            for (int i = 0; i < items.length; i++) {
                if (assignment[i][j].getValue().intValue() == 1) {
                    bin.softAdd(items[i]);
                    solution.itemMap.put(i, bin);
                }
            }
        }
        return solution;
    }
}
