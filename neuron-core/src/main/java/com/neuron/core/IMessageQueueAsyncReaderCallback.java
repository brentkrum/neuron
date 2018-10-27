package com.neuron.core;

public interface IMessageQueueAsyncReaderCallback {
	/**
	 * This msg will be released() when the onCompletedProcessing() callback has been called.
	 * If you want to keep the msg object, you must call retain() before then.
	 * 
	 * @param msg
	 */
	void submit(IMessageQueueSubmission context);
}
