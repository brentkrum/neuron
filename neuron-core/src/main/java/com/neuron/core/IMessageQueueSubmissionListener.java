package com.neuron.core;

import io.netty.util.ReferenceCounted;

/**
 * There are cases where onReceived and onStartProcessing can be called multiple
 * times (in the case of remove delivery failures and such).  You must be prepared for
 * this case.
 * 
 * There are times when onReceived and possibly onStartProcessing can be called
 * and then onUndelivered called.  Be prepared for this case.
 * 
 * Once onProcessed() or onUndelivered() are called, you are guaranteed that nothing
 * else will be called again.
 *  
 * @author brentk
 *
 */
public interface IMessageQueueSubmissionListener {
	/**
	 * The message is passed back, giving you a chance to do whatever you need with it.
	 * If you wish to keep it, you must call retain()
	 * 
	 * @param msg
	 */
	default void onReceived(ReferenceCounted msg) {
	}
	default void onStartProcessing(ReferenceCounted msg) {
	}
	
	default void onUndelivered(ReferenceCounted msg) {
	}
}