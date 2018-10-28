package com.neuron.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.BytePipeSystem.PipeBroker;
import com.neuron.core.BytePipeSystem.ReadPipeType;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.buffer.ByteBuf;

class BytePipeBufReader implements BytePipeSystem.IBytePipeReader
{
	private static final Logger LOG = LogManager.getLogger(BytePipeBufReader.class);
	
	private final EventWorker m_worker;
	private final NeuronRef m_owner;
	private final IBytePipeBufReaderCallback m_callback;
	private final ObjectConfig m_config;
	private final int m_maxBufAvailableBytes;
	private volatile BytePipeSystem.PipeBroker m_broker;
	private ByteBuf m_readBuffer = NeuronApplication.allocateIOBuffer();
	
	BytePipeBufReader(NeuronRef ref, BytePipeSystem.PipeBroker broker, ObjectConfig config, IBytePipeBufReaderCallback callback) {
		m_owner = ref;
		m_broker = broker;
		m_worker = new EventWorker(ref);
		m_config = config;
		m_callback = callback;
		m_maxBufAvailableBytes = config.getInteger(BytePipeSystem.AppendBufConfig_MaxBufAvailableBytes, 1024);
	}
	
	@Override
	public synchronized void close() {
		m_broker = null;
		m_readBuffer.release();
		m_readBuffer = null;
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
				final ByteBuf readBuffer; 
				synchronized(BytePipeBufReader.this) {
					if (m_readBuffer == null) {
						return;
					}
					readBuffer = m_readBuffer.retain(); 
				}
				try {
					while(true) {
						final ByteBuf buf = broker.dequeue();
						if (buf == null) {
							if (LOG.isTraceEnabled()) {
								LOG.trace("dequeue null, breaking loop. readableBytes={}", readBuffer.readableBytes());
							}
							break;
						}
						if (LOG.isTraceEnabled()) {
							LOG.trace("dequeue {} bytes buffer, appending to readBuffer", buf.readableBytes());
						}
						readBuffer.writeBytes(buf);
						buf.release();
						// We can get stuck in this loop if the writer is sending constant data, make a cap so we can stop
						// looping and actually process the data
						if (readBuffer.readableBytes() > m_maxBufAvailableBytes) {
							if (LOG.isTraceEnabled()) {
								LOG.trace("readableBytes[{}] > m_maxBufAvailableBytes[{}]", readBuffer.readableBytes(), m_maxBufAvailableBytes);
							}
							// Since there could be more items in the broker, we need to run this worker again
							requestMoreWork();
							break;
						}
					}
					// Get a starting count of the references to the buffer, we need to check
					// that the neuron will not be calling retain
					final int startRefCount = readBuffer.refCnt();
					
					try {
						m_callback.onData(readBuffer);
					} catch(Throwable t) {
						NeuronApplication.logError(LOG, "Uhandled exception in user provided callback", t);
					}
					
					// If the neuron called retain, we need to complain about it
					final int bufRefCnt = readBuffer.refCnt();
					if (bufRefCnt > startRefCount) {
						NeuronApplication.logWarn(LOG, "The read callback for pipe {} called retain() on the buffer passed in [startRefCount={}, buf.refCnt()={}].  This is not allowed for an AppendBuf pipe reader.  Use Chunk if you want to keep the buffers.", broker.pipeName(), startRefCount, bufRefCnt);
					} else if (bufRefCnt < startRefCount) {
						NeuronApplication.logWarn(LOG, "The read callback for pipe {} called release() on the buffer passed in [startRefCount={}, buf.refCnt()={}].  This is not allowed for an AppendBuf pipe reader.  Use Chunk if you want to use the buffer however you need.", broker.pipeName(), startRefCount, bufRefCnt);
					}
					
					readBuffer.discardReadBytes();
				} finally {
					readBuffer.release();
				}
				
			} catch(Exception ex) {
				LOG.error("Unhandled exception in reader", ex);
			}
		}
		
	}
}
