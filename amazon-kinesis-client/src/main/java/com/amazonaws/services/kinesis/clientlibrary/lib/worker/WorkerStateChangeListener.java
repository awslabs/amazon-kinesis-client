package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

/**
 * A listener for callbacks on changes worker state
 */
@FunctionalInterface
public interface WorkerStateChangeListener {
	enum WorkerState {
		CREATED,
		INITIALIZING,
		STARTED,
		SHUT_DOWN
	}

	void onWorkerStateChange(WorkerState newState);
}
