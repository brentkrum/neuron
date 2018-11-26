package com.neuron.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.MessageQueueSystemBase.QueueBroker;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.util.ReferenceCounted;

class MessageQueueReader implements MessageQueueSystemBase.IMessageReader
{
	private static final Logger LOG = LogManager.getLogger(MessageQueueReader.class);
	
	private final EventWorker m_worker;
	private final NeuronRef m_owner;
	private final IDuplexMessageQueueReaderCallback m_callback;
	private volatile QueueBroker m_broker;
	
	MessageQueueReader(NeuronRef ref, QueueBroker broker, ObjectConfig config, IDuplexMessageQueueReaderCallback callback) {
		m_owner = ref;
		m_broker = broker;
		m_worker = new EventWorker(ref);
		m_callback = callback;
	}
	
	@Override
	public synchronized void close() {
		m_broker = null;
	}

	@Override
	public NeuronRef owner()
	{
		return m_owner;
	}

	@Override
	public void onEvent(Event event)
	{
		m_worker.requestMoreWork();
	}
	
	private class EventWorker extends PerpetualWorkContextAware {

		public EventWorker(NeuronRef ref) {
			super(ref);
		}

		@Override
		protected void _lockException(Exception ex) {
			LOG.fatal("Unexpected locking exception", ex);
		}

		@Override
		protected void _doWork(INeuronStateLock neuronLock)
		{
			// We only deliver data to SystemOnline, Online and GoingOffline neurons
			if (!neuronLock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
				return;
			}
			try {
				final QueueBroker broker = m_broker;
				if (broker == null) {
					return;
				}
				final IMessageQueueSubmission submission = broker.dequeue();
				if (submission == null) {
					return;
				}
				
				try {
					final ReferenceCounted req = submission.startProcessing();
					if (req == null) {
						return;
					}
					final ReferenceCounted res = m_callback.onData(req);
					submission.setAsProcessed( res );
				} catch(Throwable t) {
					NeuronApplication.logError(LOG, "Unhandled exception in user provided callback, returning null response message", t);
					// We don't want to call cancelMessage here since we don't want to create an infinite loop of exceptions thrown from onData with
					// the same set of inputs being repeatedly fed back into it
					submission.setAsProcessed(null);
				}
				requestMoreWork();
				
			} catch(Exception ex) {
				LOG.error("Unhandled exception in reader", ex);
			}
		}
		
	}
}
