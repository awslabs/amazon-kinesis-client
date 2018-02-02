package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker.WorkerStateChangeListener;

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
