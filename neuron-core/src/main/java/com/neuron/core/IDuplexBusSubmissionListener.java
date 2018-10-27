package com.neuron.core;

import io.netty.util.ReferenceCounted;

/**
 * There are cases where onReceived and onStartProcessing can be called multiple
 * times (in the case of remove delivery failures and such).  You must be prepared for
 * this case. onReset() will be called before onReceived or onStartProcessing are
 * called for a possible second time.
 * 
 * There are times when onReceived and possibly onStartProcessing can be called
 * and then onUndelivered called.  Be prepared for this case.  
 * 
 * Once one of onProcessed(), onSystemFailure(), or onUndelivered() are called, you
 * are guaranteed that nothing else will be called again.
 *  
 * @author brentk
 *
 */
public interface IDuplexBusSubmissionListener {
	/**
	 * The message is passed back, giving you a chance to do whatever you need with it.
	 * If you wish to keep it, you must call retain()
	 * 
	 * @param msg
	 */
	default void onReceived(ReferenceCounted requestMsg) {
	}
	default void onStartProcessing(ReferenceCounted requestMsg) {
	}
	default void onReset(ReferenceCounted requestMsg) {
	}
	
	default void onProcessed(ReferenceCounted requestMsg, ReferenceCounted responseMsg) {
	}
	default void onSystemFailure(ReferenceCounted requestMsg) {
	}
	default void onUndelivered(ReferenceCounted requestMsg) {
	}
}