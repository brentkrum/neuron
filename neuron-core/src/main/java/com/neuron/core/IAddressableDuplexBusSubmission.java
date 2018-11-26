package com.neuron.core;

import io.netty.util.ReferenceCounted;

public interface IAddressableDuplexBusSubmission {
	/**
	 * This can return 0 under certain edge cases when a Neuron is disconnecting.  An id
	 * of 0 is not a valid id.
	 *  
	 */
	int id();
	void setAsReceived();
	
	/**
	 * This can return null under certain edge cases when a Neuron is disconnecting.
	 */
	ReferenceCounted startProcessing();

	/**
	 * This is used to return the message to the queue as unprocessed after startProcessing() has been called.
	 */
	void cancelProcessing();
	
	void setAsProcessed(ReferenceCounted responseMsg);
	void setAsFailed(Throwable t);
}