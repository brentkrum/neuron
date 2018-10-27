package com.neuron.core;

import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;

public interface IAddressableDuplexBusListener {
	/**
	 * This request will be released() when the future is set as successful or failed.
	 * If you want to keep the request object beyond that, you must call retain() before
	 * calling setSuccessful or setFailed on the promise.
	 * 
	 * @param msg
	 */
	Future<ReferenceCounted> onRequest(ReferenceCounted request);
}
