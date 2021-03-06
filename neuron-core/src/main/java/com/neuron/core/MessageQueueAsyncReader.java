package com.neuron.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.MessageQueueSystemBase.QueueBroker;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

class MessageQueueAsyncReader implements MessageQueueSystemBase.IMessageReader
{
	private static final Logger LOG = LogManager.getLogger(MessageQueueAsyncReader.class);
	
	private final EventWorker m_worker;
	private final NeuronRef m_owner;
	private final IMessageQueueAsyncReaderCallback m_callback;
	private final NeuronState[] m_onlineStates;
	private QueueBroker m_broker;
	
	MessageQueueAsyncReader(NeuronRef ref, QueueBroker broker, ObjectConfig config, IMessageQueueAsyncReaderCallback callback) {
		m_owner = ref;
		m_broker = broker;
		m_worker = new EventWorker(ref);
		m_callback = callback;
		Object o = config.getPOJO(MessageQueueSystemBase.queueBrokerConfig_OnlineStates);
		if (o instanceof NeuronState[]) {
			m_onlineStates = (NeuronState[])o;
		} else {
			m_onlineStates = new NeuronState[] {NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline};
		}
	}
	
	@Override
	public void close() {
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
			if (!neuronLock.isStateOneOf(m_onlineStates)) {
				return;
			}
			
			// We cannot be here if close() was called, since our state will be Disconnecting
			// It is safe to use m_broker
			try {
				final IMessageQueueSubmission currentSubmission = m_broker.checkout();
				if (currentSubmission == null) {
					return;
				}
				try {
					m_callback.submit(currentSubmission);
				} catch(Throwable t) {
					NeuronApplication.logError(LOG, "Unhandled exception in user provided callback", t);
				}
				requestMoreWork();

			} catch(Exception ex) {
				LOG.error("Unhandled exception in reader", ex);
			}
		}
		
	}
}
