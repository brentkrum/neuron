package com.neuron.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.BytePipeSystem.PipeBroker;
import com.neuron.core.BytePipeSystem.ReadPipeType;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.buffer.ByteBuf;

class BytePipeChunkReader implements BytePipeSystem.IBytePipeReader
{
	private static final Logger LOG = LogManager.getLogger(BytePipeChunkReader.class);
	
	private final EventWorker m_worker;
	private final NeuronRef m_owner;
	private final IBytePipeBufReaderCallback m_callback;
	private final ObjectConfig m_config;
	private volatile BytePipeSystem.PipeBroker m_broker;
	
	BytePipeChunkReader(NeuronRef ref, BytePipeSystem.PipeBroker broker, ObjectConfig config, IBytePipeBufReaderCallback callback) {
		m_owner = ref;
		m_broker = broker;
		m_worker = new EventWorker(ref);
		m_config = config;
		m_callback = callback;
	}
	
	@Override
	public synchronized void close() {
		m_broker = null;
	}
	
	@Override
	public void replaceBroker(PipeBroker newBroker) {
		m_broker = newBroker;
	}

	@Override
	public Object callback() {
		return m_callback;
	}

	@Override
	public ReadPipeType pipeType() {
		return ReadPipeType.AppendBuf;
	}

	@Override
	public ObjectConfig config() {
		return m_config;
	}

	@Override
	public NeuronRef owner()
	{
		return m_owner;
	}

	@Override
	public boolean writeThrough(ByteBuf buf)
	{
		return false;
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
				final BytePipeSystem.PipeBroker broker = m_broker;
				if (broker == null) {
					return;
				}
				final ByteBuf buf = broker.dequeue();
				if (buf == null) {
					return;
				}
				try {
					m_callback.onData(buf);
				} catch(Throwable t) {
					NeuronApplication.logError(LOG, "Uhandled exception in user provided callback", t);
				}
				buf.release();
				requestMoreWork();
				
			} catch(Exception ex) {
				LOG.error("Unhandled exception in reader", ex);
			}
		}
		
	}
}
