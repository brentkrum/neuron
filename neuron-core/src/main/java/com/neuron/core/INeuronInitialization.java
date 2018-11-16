package com.neuron.core;

import io.netty.util.concurrent.Promise;

public interface INeuronInitialization extends INeuron
{
	default void init(Promise<Void> promise) {
		promise.setSuccess((Void)null);
	}
	default long initTimeoutInMS() {
		return 1000;
	}
	
	/**
	 * This method is called in the case where the init method
	 * does not return within the time limit set out by initTimeoutInMS().
	 * It should be used to stop the async processes/tasks started by
	 * the init method and clean up any resources.
	 * 
	 * This will be the last method called in this neuron's lifecycle,
	 * since it is considered to be in a failed init state and this generation
	 * is marked offline at the time of the call to this method.
	 * 
	 * The neuron will still be in a GoingOnline state while this method
	 * runs.
	 * 
	 */
	default void onInitTimeout() {
	}
	
	default void goingOffline(Promise<Void> promise) {
		promise.setSuccess((Void)null);
	}
	default long goingOfflineTimeoutInMS() {
		return 5000;
	}
	default void onGoingOfflineTimeout() {
	}
	
	default void deinit(Promise<Void> promise) {
		promise.setSuccess((Void)null);
	}
	default long deinitTimeoutInMS() {
		return 5000;
	}
	default void onDeinitTimeout() {
	}
	
	default void connectResources() {
		return;
	}

	/**
	 * This method is called by the single online/offline thread. Any time
	 * spent in this method prevents any other neurons or templates from
	 * going online or offline.  This also guarantees that this method
	 * will run without the risk of needing to synchronize with the deinit
	 * method since it cannot be called while this method runs.
	 * 
	 * This method should do no blocking operations or long running
	 * code.  It is meant to start async processes or set flag, etc.
	 * If you need to do something long-running, you should
	 * submit a Runnable into the Task pool.
	 * 
	 */
	default void nowOnline() {
	}
	
}
