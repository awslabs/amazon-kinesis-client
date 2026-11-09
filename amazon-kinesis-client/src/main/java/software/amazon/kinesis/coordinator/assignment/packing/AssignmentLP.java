package software.amazon.kinesis.coordinator.assignment.packing;

import java.math.BigDecimal;

import org.ojalgo.optimisation.Expression;
import org.ojalgo.optimisation.ExpressionsBasedModel;
import org.ojalgo.optimisation.Optimisation;
import org.ojalgo.optimisation.Variable;
import software.amazon.kinesis.coordinator.assignment.packing.BinPackingModel.Packing;

public class AssignmentLP {

    ExpressionsBasedModel model;

    Variable[][] assignments;
    Expression[] constraints;
    Expression objective;

    Optimisation.Result result;

    int maxItems;
    int numItems;
    int numMetrics;
    int numBins;

    double scores[][];
    long capacities[][];

    public AssignmentLP(int maxItems, int numBins, int numMetrics) {
        this.maxItems = maxItems;
        this.numBins = numBins;
        this.numMetrics = numMetrics;

        init();
    }

    private void init() {
        scores = new double[maxItems][numBins];
        capacities = new long[numBins][numMetrics];

        model = new ExpressionsBasedModel();

        assignments = new Variable[maxItems][numBins];
        constraints = new Expression[maxItems];

        for (int i = 0; i < maxItems; i++) {
            for (int j = 0; j < numBins; j++) {
                assignments[i][j] =
                        model.addVariable("assignments_" + i + "_" + j).lower(BigDecimal.ZERO);
            }
        }

        for (int i = 0; i < maxItems; i++) {
            constraints[i] = model.addExpression("constraints_" + i);
        }

        objective = model.addExpression("objective");
    }

    public Optimisation.Result solve() {
        return (result = model.minimise());
    }

    public void compile() {
        constraints();
        objective();
    }

    private void constraints() {
        // each item to exactly 1 bin
        for (int i = 0; i < maxItems; i++) {
            constraints[i].level(BigDecimal.ONE);

            for (int j = 0; j < numBins; j++) {
                constraints[i].set(assignments[i][j], BigDecimal.ONE);
            }
        }
    }

    private void objective() {
        // multiply scores by assignment variables
        for (int i = 0; i < maxItems; i++) {
            for (int j = 0; j < numBins; j++) {
                objective.set(assignments[i][j], BigDecimal.valueOf(scores[i][j]));
            }
        }

        objective.weight(BigDecimal.ONE);
    }

    void extract(Packing packing) {
        for (int i = 0; i < numItems; i++) {
            for (int j = 0; j < numBins; j++) {
                if (assignments[i][j].getValue().compareTo(BigDecimal.valueOf(0.5)) > 0) { // compare with tolerance
                    packing.softAdd(i, j);
                }
            }
        }
    }
}
