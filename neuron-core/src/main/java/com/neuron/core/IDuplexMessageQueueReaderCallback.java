package com.neuron.core;

import io.netty.util.ReferenceCounted;

public interface IDuplexMessageQueueReaderCallback {
	/**
	 * This msg will be released() on return from this method.  If you want to keep
	 * the msg object, you must call retain().
	 * 
	 * @param msg
	 */
	ReferenceCounted onData(ReferenceCounted msg);
}
