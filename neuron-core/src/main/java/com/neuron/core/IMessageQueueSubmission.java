package com.neuron.core;

import io.netty.util.ReferenceCounted;

public interface IMessageQueueSubmission {
	/**
	 * This can return 0 under certain edge cases when a Neuron is disconnecting.  An id
	 * of 0 is not a valid id.
	 *  
	 */
	int id();
	void setAsReceived();
	void setAsStartedProcessing();
	void setAsProcessed(ReferenceCounted response);
	/**
	 * This can return null under certain edge cases when a Neuron is disconnecting.
	 */
	ReferenceCounted message();
}