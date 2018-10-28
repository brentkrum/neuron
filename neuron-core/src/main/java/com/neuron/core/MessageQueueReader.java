package com.neuron.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.MessageQueueSystem.QueueBroker;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

class MessageQueueReader implements MessageQueueSystem.IMessageReader
{
	private static final Logger LOG = LogManager.getLogger(MessageQueueReader.class);
	
	private final EventWorker m_worker;
	private final NeuronRef m_owner;
	private final IMessageQueueReaderCallback m_callback;
	private volatile QueueBroker m_broker;
	
	MessageQueueReader(NeuronRef ref, QueueBroker broker, ObjectConfig config, IMessageQueueReaderCallback callback) {
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
				submission.setAsStartedProcessing();
				try {
					m_callback.onData(submission.message());
				} catch(Throwable t) {
					NeuronApplication.logError(LOG, "Unhandled exception in user provided callback", t);
				}
				submission.setAsProcessed();
				requestMoreWork();
				
			} catch(Exception ex) {
				LOG.error("Unhandled exception in reader", ex);
			}
		}
		
	}
}
