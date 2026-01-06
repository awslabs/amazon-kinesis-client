package software.amazon.kinesis.coordinator.assignment.packing;

public final class BinPackingOptions {

    public enum SolveStrategy {
        /** gets base state with first-fit-decreasing and feeds to integer program */
        IP_WITH_FFD,
        /** minimizes slack with integer program; can penalize/limit reassignments from provided base state */
        IP,
        /** gets base state with first-fit-decreasing and feeds to simulated annealing */
        ANNEAL_WITH_FFD,
        /** minimizes slack with simulated annealing; needs base state provided */
        ANNEAL,
        /** runs first-fit-decreasing algorithm */
        FFD
    }

    public enum Objective {
        /** minimizes sum of slack variables, plus other objectives like minimum bins */
        TOTAL_SLACK,
        /** minimizes sum of squares of slack variables (makes objective quadratic but improves balance) */
        LEAST_SQUARES,
        /** ignores slack and finds feasible solution, respects other objectives like minimum bins */
        FEASIBLE
    }

    public enum NumBinsStrategy {
        /** binary search for minimum bins using integer program feasibility checks */
        PRECHECK,
        /** balances bins with slack during main solve according to bin cost provided */
        FLEXIBLE,
        /** takes hard-coded number of bins; if infeasible due to hard capacity, switches to overfill slack */
        PROVIDED
    }

    public enum CapacityMode {
        /** captures slack on both sides of capacity constraints */
        SOFT,
        /** captures underfill slack only; switches to overfill if infeasible number of bins provided */
        HARD,
        /** use with flexible bins; soft constraints but auto-computes target capacities as average / numBins */
        AUTO
    }

    public enum ReassignmentStrategy {
        /** does not track differences in assignments versus base state */
        NONE,
        /** adds weighted penalty per reassignment to the objective; if no base state provided, must use FFD */
        PENALTY,
        /** hard limit on number of reassignments; also needs base state */
        LIMIT,
        /** adds weighted penalty per excess reassignment to the objective; also needs base state */
        BOTH
    }
}
