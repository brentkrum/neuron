package com.neuron.utility;

public class AsynchronousHandlerClose {
	private final ICleanupHandler m_handler;
	private int m_numRunning;
	private boolean m_closed;
	
	public AsynchronousHandlerClose(ICleanupHandler cleanupHandler) {
		m_handler = cleanupHandler;
	}

	/**
	 * runMe will be run within a synchronized section that will block
	 * others from doing their work.  Only do very short simple things
	 * in there.
	 * 
	 * @param runMe
	 */
	public void ifNotClosed(Runnable runMe) {
		synchronized(this) {
			if (m_closed) {
				return;
			}
			runMe.run();
		}
	}

	public boolean isClosed() {
		synchronized(this) {
			return m_closed;
		}		
	}

	public boolean handlerStart() {
		synchronized(this) {
			if (m_closed) {
				return false;
			}
			m_numRunning++;
		}
		return true;
	}
	
	public boolean handlerClose() {
		synchronized(this) {
			if (m_closed) {
				return false;
			}
			m_closed = true;
		}
		return true;
	}
	
	public void handlerFinish() {
		final boolean callCleanup;
		synchronized(this) {
			m_numRunning--;
			callCleanup = (m_numRunning == 0 && m_closed);
		}
		if (callCleanup) {
			m_handler.cleanup();
		}
	}
	
	public boolean externalClose() {
		final boolean callCleanup;
		synchronized(this) {
			if (m_closed) {
				return false;
			}
			m_closed = true;
			callCleanup = (m_numRunning == 0);
		}
		if (callCleanup) {
			m_handler.cleanup();
		}
		return true;
	}
	
	public interface ICleanupHandler {
		void cleanup();
	}
}
