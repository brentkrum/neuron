package com.neuron.core;

import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;

public final class DuplexMessageQueueSystem extends MessageQueueSystemBase
{
	public static final String queueBrokerConfig_MaxMsgCount = MessageQueueSystemBase.queueBrokerConfig_MaxMsgCount;
	public static final String queueBrokerConfig_MaxSimultaneousCheckoutCount = MessageQueueSystemBase.queueBrokerConfig_MaxSimultaneousCheckoutCount;

	private DuplexMessageQueueSystem() {
	}

	static void register() {
		NeuronApplication.register(new Registrant());
	}
	
	public static String createFQQN(String declaringNeuronInstanceName, String queueName) {
		return MessageQueueSystemBase.createFQQN(declaringNeuronInstanceName, queueName);
	}
	
	public static void defineQueue(String queueName, ObjectConfig queueConfig, IDuplexMessageQueueReaderCallback callback) {
		MessageQueueSystemBase.defineQueue(queueName, queueConfig, callback);
	}
	
	public static void defineQueue(String queueName, ObjectConfig queueConfig, IMessageQueueAsyncReaderCallback callback) {
		MessageQueueSystemBase.defineQueue(queueName, queueConfig, callback);
	}
	
	/**
	 * 
	 * @param declaringNeuronInstanceName - name of the neuron who owns the queue and configures it (the one who did or will call defineQueue)
	 * @param queueName - the name of the queue.
	 * @param msg - you are passing your reference into the queue, the Queue does not call retain()
	 * @param listener - callback for progress of the message.  May be null if you don't care.  The listener
	 *  is responsible for setting and checking the state of a Neuron via a state lock. 
	 * 
	 * @return true if it was added to the queue, false otherwise.  When false, the Queue did not
	 *  do anything with msg, the reference still stands and is now re-owned by the caller.
	 *  
	 */
	public static boolean submitToQueue(String declaringNeuronInstanceName, String queueName, ReferenceCounted msg, IDuplexMessageQueueSubmissionListener listener) {
		return MessageQueueSystemBase.submitToQueue(declaringNeuronInstanceName, queueName, msg, listener);
	}
	
	/**
	 * 
	 * @param fqqn - fully qualified queue name
	 * @param msg - you are passing your reference into the queue, the Queue does not call retain()
	 * @param listener - callback for progress of the message.  May be null if you don't care.  The listener
	 *  is responsible for setting and checking the state of a Neuron via a state lock. 
	 * 
	 * @return true if it was added to the queue, false otherwise.  When false, the Queue did not
	 *  do anything with msg, the reference still stands and is now re-owned by the caller.
	 *  
	 */
	public static boolean submitToQueue(String fqqn, ReferenceCounted msg, IMessageQueueSubmissionListener listener) {
		return MessageQueueSystemBase.submitToQueue(fqqn, msg, listener);
	}
	
	private static class Registrant implements INeuronApplicationSystem {

		@Override
		public String systemName()
		{
			return "DuplexMessageQueueSystem";
		}

		@Override
		public Future<Void> startShutdown() {
			return MessageQueueSystemBase.startShutdown();
		}
	}
}
