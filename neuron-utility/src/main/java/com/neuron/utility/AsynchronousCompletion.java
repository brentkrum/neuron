package com.neuron.utility;

public class AsynchronousCompletion {
	private final IOnAllCompletionsFinished m_callback;
	private int m_numExpectedCompletions;
	private int m_numCompletionsSoFar;
	private boolean m_allCompletionsDone;
	
	public AsynchronousCompletion(IOnAllCompletionsFinished callback) {
		m_callback = callback;
	}

	public void reset() {
		synchronized(this) {
			m_allCompletionsDone = false;
			m_numExpectedCompletions = 0;
			m_numCompletionsSoFar = 0;
		}
	}
	
	public void incrementCompletions() {
		synchronized(this) {
			if (m_allCompletionsDone) {
				throw new java.lang.IllegalStateException("All have already completed");
			}
			m_numExpectedCompletions ++;
		}
	}
	
	public void incrementCompletions(int byCount) {
		synchronized(this) {
			if (m_allCompletionsDone) {
				throw new java.lang.IllegalStateException("All have already completed");
			}
			m_numExpectedCompletions += byCount;
		}
	}
	
	public void completed() {
		boolean callFinished = false;
		synchronized(this) {
			if (m_allCompletionsDone) {
				throw new java.lang.IllegalStateException("All have already completed");
			}
			m_numCompletionsSoFar++;
			if (m_numCompletionsSoFar == m_numExpectedCompletions) {
				m_allCompletionsDone = true;
				callFinished = true;
			}
		}
		if (callFinished) {
			// Let exceptions blow out to the caller to completed()
			m_callback.finished();
//			try {
//				ServiceBusApplication.getExecutorService().execute(() -> {
//					m_callback.finished();
//				});
//			} catch(Exception ex) {
//				m_callback.finished();
//			}
		}
	}
	
	public interface IOnAllCompletionsFinished {
		void finished();
	}
}
