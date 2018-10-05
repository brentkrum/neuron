package com.neuron.utility;

import java.util.HashMap;
import java.util.Map;

public class AsynchronousContributions {
	private final IOnContributionsFinished m_callback;
	private final boolean m_allowDuplicateContributions;
	private int m_numContributors;
	private int m_numContributions;
	private Map<String, Object> m_contributions = new HashMap<String,Object>();
	private boolean m_allContributionsDone;
	
	public AsynchronousContributions(IOnContributionsFinished callback) {
		m_callback = callback;
		m_allowDuplicateContributions = false;
	}

	public AsynchronousContributions(IOnContributionsFinished callback, boolean allowDuplicateContributions) {
		m_callback = callback;
		m_allowDuplicateContributions = allowDuplicateContributions;
	}

	public void reset() {
		synchronized(this) {
			m_allContributionsDone = false;
			m_contributions.clear();
			m_numContributors = 0;
			m_numContributions = 0;
		}
	}
	
	public void incrementContributors() {
		synchronized(this) {
			if (m_allContributionsDone) {
				throw new java.lang.IllegalStateException("All contributions have already been made");
			}
			m_numContributors ++;
		}
	}
	
	public void incrementContributors(int byCount) {
		synchronized(this) {
			if (m_allContributionsDone) {
				throw new java.lang.IllegalStateException("All contributions have already been made");
			}
			m_numContributors += byCount;
		}
	}
	
	public void contribute(String resultKey, Object resultData) {
		boolean callFinished = false;
		synchronized(this) {
			if (m_allContributionsDone) {
				throw new java.lang.IllegalStateException("All contributions have already been made");
			}
			final Object existing = m_contributions.put(resultKey, resultData);
			if (existing != null && !m_allowDuplicateContributions) {
				m_contributions.put(resultKey, existing); // Restore previous
				throw new java.lang.IllegalStateException("A contribution for the result key '" + resultKey + "' has already been made");
			}
			m_numContributions++;
			if (m_numContributions == m_numContributors) {
				m_allContributionsDone = true;
				callFinished = true;
			}
		}
		if (callFinished) {
			// Let exceptions blow out to the caller to contribute
			m_callback.finished(m_contributions);
// I am going to leave this here for now, in case we want to go back to calling finished on another thread
//			try {
//				ServiceBusApplication.getExecutorService().execute(() -> {
//					try {
//						m_callback.finished(m_contributions);
//					} catch (Exception ex) {
//						// What to do here??
//					}
//				});
//			} catch(Exception ex) { // The only exception that should be thrown here is a RejectedExecutionException
//			}
		}
	}
	
	public interface IOnContributionsFinished {
		void finished(Map<String, Object> contributions);
	}
}
