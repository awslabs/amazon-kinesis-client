package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

public class NoOpWorkerStateChangeListener implements WorkerStateChangeListener {

	/**
	 * Empty constructor for NoOp Worker State Change Listener
	 */
	public NoOpWorkerStateChangeListener() {

	}

	@Override
	public void onWorkerStateChange(WorkerState newState) {

	}
}
