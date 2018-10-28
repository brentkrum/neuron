package com.neuron.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;

class AddressableDuplexBusReader implements AddressableDuplexBusSystem.IMessageReader
{
	private static final Logger LOG = LogManager.getLogger(AddressableDuplexBusReader.class);
	
	private final EventWorker m_worker;
	private final NeuronRef m_owner;
	private final IAddressableDuplexBusListener m_listener;
	private volatile AddressableDuplexBusSystem.ReaderBroker m_broker;
	
	AddressableDuplexBusReader(NeuronRef ref, AddressableDuplexBusSystem.ReaderBroker broker, ObjectConfig config, IAddressableDuplexBusListener listener) {
		m_owner = ref;
		m_broker = broker;
		m_worker = new EventWorker(ref);
		m_listener = listener;
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
				final AddressableDuplexBusSystem.ReaderBroker broker = m_broker;
				if (broker == null) {
					return;
				}
				final IAddressableDuplexBusSubmission submission = broker.dequeue();
				if (submission == null) {
					return;
				}
				submission.setAsStartedProcessing();
				try {
					m_listener.onRequest(submission.message()).addListener((Future<ReferenceCounted> requestFuture) -> {
						try {
							if (requestFuture.isSuccess()) {
								submission.setAsProcessed(requestFuture.getNow());
							} else {
								submission.setAsFailed(requestFuture.cause());
							}
						} catch(Exception ex) {
							// This really should never happen, but if it does we want to know
							LOG.error("Unhandled exception in submission", ex);
						}
					});
				} catch(Throwable t) {
					NeuronApplication.logError(LOG, "Unhandled exception in neuron provided callback", t);
					submission.setAsFailed(t);
				}
				requestMoreWork();
				
			} catch(Exception ex) {
				LOG.error("Unhandled exception in reader", ex);
			}
		}
		
	}
}
