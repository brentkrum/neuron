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
	
	/**
	 * This can return null under certain edge cases when a Neuron is disconnecting.
	 */
	ReferenceCounted startProcessing();
	
	/**
	 * For simplex message queues, pass null as the response
	 *  
	 * @param response
	 */
	void setAsProcessed(ReferenceCounted response);
	
	/**
	 * This can be used during shutdown after setAsStartedProcessing has been called to
	 * put the mssage back into the queue 
	 */
	void cancelProcessing();
}