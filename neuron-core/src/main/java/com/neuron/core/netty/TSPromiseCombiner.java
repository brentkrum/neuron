package com.neuron.core.netty;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseCombiner;
import io.netty.util.internal.ObjectUtil;

public class TSPromiseCombiner {
   private int expectedCount;
   private int doneCount;
   private boolean doneAdding;
   private Promise<Void> aggregatePromise;
   private Throwable cause;
   private final GenericFutureListener<Future<?>> listener = new GenericFutureListener<Future<?>>() {
       @Override
       public void operationComplete(Future<?> future) throws Exception {
      	 final boolean callPromise;
      	 synchronized(TSPromiseCombiner.this) {
	           ++doneCount;
	           if (!future.isSuccess() && cause == null) {
	               cause = future.cause();
	           }
	           callPromise = (doneCount == expectedCount && doneAdding);
      	 }
      	 if (callPromise) {
             tryPromise();
      	 }
       }
   };

   /**
    * Adds a new future to be combined. New futures may be added until an aggregate promise is added via the
    * {@link PromiseCombiner#finish(Promise)} method.
    *
    * @param future the future to add to this promise combiner
    */
   @SuppressWarnings({ "unchecked", "rawtypes" })
   public void add(Future future) {
       checkAddAllowed();
       ++expectedCount;
       future.addListener(listener);
   }

   /**
    * Adds new futures to be combined. New futures may be added until an aggregate promise is added via the
    * {@link PromiseCombiner#finish(Promise)} method.
    *
    * @param futures the futures to add to this promise combiner
    */
   @SuppressWarnings({ "rawtypes" })
   public void addAll(Future... futures) {
       for (Future future : futures) {
           this.add(future);
       }
   }

   /**
    * <p>Sets the promise to be notified when all combined futures have finished. If all combined futures succeed,
    * then the aggregate promise will succeed. If one or more combined futures fails, then the aggregate promise will
    * fail with the cause of one of the failed futures. If more than one combined future fails, then exactly which
    * failure will be assigned to the aggregate promise is undefined.</p>
    *
    * <p>After this method is called, no more futures may be added via the {@link PromiseCombiner#add(Future)} or
    * {@link PromiseCombiner#addAll(Future[])} methods.</p>
    *
    * @param aggregatePromise the promise to notify when all combined futures have finished
    */
   public void finish(Promise<Void> aggregatePromise) {
   	final boolean callPromise;
   	synchronized(TSPromiseCombiner.this) {
   		if (doneAdding) {
   			throw new IllegalStateException("Already finished");
   		}
   		doneAdding = true;
   		this.aggregatePromise = ObjectUtil.checkNotNull(aggregatePromise, "aggregatePromise");
   		callPromise = (doneCount == expectedCount);
   	}
   	if (callPromise) {
			tryPromise();
   	}
   }

   private boolean tryPromise() {
       return (cause == null) ? aggregatePromise.trySuccess(null) : aggregatePromise.tryFailure(cause);
   }

   private void checkAddAllowed() {
       if (doneAdding) {
           throw new IllegalStateException("Adding promises is not allowed after finished adding");
       }
   }
}
