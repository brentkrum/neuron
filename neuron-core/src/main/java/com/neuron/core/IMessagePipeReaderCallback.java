package com.neuron.core;

import io.netty.util.ReferenceCounted;

public interface IMessagePipeReaderCallback {
	/**
	 * This msg will be released() on return from this method.  If you want to keep
	 * the msg object, you must call retain().
	 * 
	 * @param msg
	 */
	void onData(ReferenceCounted msg);
}
