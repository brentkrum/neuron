package com.neuron.core;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.FastLinkedList;
import com.neuron.utility.IntTrie;

public class StatusSystem
{
	public enum StatusType { Up, Down, Intermittent, Online, Offline, GoingOnline, GoingOffline} ;
	
	private static final Logger LOG = LogManager.getLogger(StatusSystem.class);
	private static final int TEMPLATE_STATUS_TRAIL_SIZE = Config.getFWInt("core.StatusSystem.templateStatusTrailSize", 16);
	
//	private static final ReadWriteLock m_typeLock = new ReentrantReadWriteLock(true);
//	private static final CharSequenceTrie<String> m_types = new CharSequenceTrie<>();
//	private static final List<String> m_typesList = new LinkedList<>();
	
	private static final Object m_templateLock = new Object();
	private static final IntTrie<TemplateStatusHolder> m_templateStatusTrie = new IntTrie<>();
	
	private static final Object m_neuronLock = new Object();
	private static final IntTrie<NeuronStatusHolder> m_neuronStatusTrie = new IntTrie<>();
	
	private static final Object m_hostLock = new Object();
	private static final CharSequenceTrie<HostStatusHolder> m_hostStatusTrie = new CharSequenceTrie<>();
	
//	static {
//		m_types.addOrFetch(NEURON_TEMPLATE_TYPE_NAME, "");
//		m_typesList.add(NEURON_TEMPLATE_TYPE_NAME);
//		m_types.addOrFetch(NEURON_INSTANCE_TYPE_NAME, "");
//		m_typesList.add(NEURON_INSTANCE_TYPE_NAME);
//	}

	static void register() {
		NeuronApplication.register(new Registrant());
	}
//	
//	public static void setStatus(String neuronTemplateName, int neuronTemplateId, String neuronInstanceName, int neuronInstanceId, String status) {
//	}
//	
//	public static void setStatus(int neuronTemplateId, int neuronInstanceId, String status) {
//		String neuronTemplateName = NeuronSystem.getNeuronTemplateName(neuronTemplateId);
//		//String neuronInstanceName = NeuronSystem.getNeuronName(neuronTemplateId);
//		String neuronInstanceName = null;
//		setStatus(neuronTemplateName, neuronTemplateId, neuronInstanceName, neuronInstanceId, status);
//	}
	public static void setStatus(TemplateRef ref, StatusType status, String reasonText) {
		synchronized(m_templateLock) {
			TemplateStatusHolder h = m_templateStatusTrie.get(ref.id());
			if (h == null) {
				h = new TemplateStatusHolder(ref);
				m_templateStatusTrie.addOrFetch(ref.id(), h);
			}
			h.addStatusTrail(status, reasonText);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: {} {}", ref.logString(), status, reasonText);
		}
	}
	
	public static void setStatus(NeuronRef instanceRef, StatusType status, String reasonText) {
		synchronized(m_neuronLock) {
			NeuronStatusHolder h = m_neuronStatusTrie.get(instanceRef.id());
			if (h == null) {
				h = new NeuronStatusHolder(instanceRef);
				m_neuronStatusTrie.addOrFetch(instanceRef.id(), h);
			}
			h.addStatusTrail(status, reasonText);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("({}: {} {}", instanceRef.logString(), status, reasonText);
		}
	}

	public static void setHostStatus(String hostAndPort, StatusType status, String reasonText) {
		synchronized(m_hostLock) {
			HostStatusHolder h = m_hostStatusTrie.get(hostAndPort);
			if (h == null) {
				h = new HostStatusHolder(false, hostAndPort);
				m_hostStatusTrie.addOrFetch(hostAndPort, h);
			}
			h.addStatusTrail(status, reasonText);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: {} {}", hostAndPort, status, reasonText);
		}
	}

	public static void setInboundStatus(String hostAndPort, StatusType status, String reasonText) {
		synchronized(m_hostLock) {
			HostStatusHolder h = m_hostStatusTrie.get(hostAndPort);
			if (h == null) {
				h = new HostStatusHolder(true, hostAndPort);
				m_hostStatusTrie.addOrFetch(hostAndPort, h);
			}
			h.addStatusTrail(status, reasonText);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: {} {}", hostAndPort, status, reasonText);
		}
	}

//	public static List<String> getStatusTypes() {
//		final List<String> out;
//		m_typeLock.readLock().lock();
//		try {
//			out = new ArrayList<String>(m_typesList.size());
//			for(String s : m_typesList) {
//				out.add(s);
//			}
//		} finally {
//			m_typeLock.readLock().unlock();
//		}
//		return out;
//	}
	
	public static List<CurrentStatus> getCurrentStatus() {
		final List<CurrentStatus> out = new LinkedList<>();
		synchronized(m_templateLock) {
			m_templateStatusTrie.forEach((h) -> {
				CurrentStatus cs = h.getCurrentStatus();
				if (cs != null) {
					out.add(cs);
				}
				return true;
			});
		}
		synchronized(m_neuronLock) {
			m_neuronStatusTrie.forEach((h) -> {
				CurrentStatus cs = h.getCurrentStatus();
				if (cs != null) {
					out.add(cs);
				}
				return true;
			});
		}
		synchronized(m_hostLock) {
			m_hostStatusTrie.forEach((key, value) -> {
				CurrentStatus cs = value.getCurrentStatus();
				if (cs != null) {
					out.add(cs);
				}
				return true;
			});
		}
		return out;
	}
	
	public static class CurrentStatus {
		public final long timestamp;
		public final StatusType status;
		public final String reasonText;
		
		CurrentStatus(long timestamp, StatusType status, String reasonText) {
			this.timestamp = timestamp;
			this.status = status;
			this.reasonText = reasonText;
		}
	}
	
	public static class CurrentNeuronStatus extends CurrentStatus {
		public final NeuronRef neuronRef;
		
		CurrentNeuronStatus(long timestamp, StatusType status, String reasonText, NeuronRef ref) {
			super(timestamp, status, reasonText);
			this.neuronRef = ref;
		}
		
	}
	
	public static class CurrentTemplateStatus extends CurrentStatus {
		public final TemplateRef templateRef;
		
		CurrentTemplateStatus(long timestamp, StatusType status, String reasonText, TemplateRef ref) {
			super(timestamp, status, reasonText);
			this.templateRef = ref;
		}
		
	}
	
	public static class CurrentHostStatus extends CurrentStatus {
		public final boolean isInbound;
		public final String hostAndPort;
		
		CurrentHostStatus(long timestamp, StatusType status, String reasonText, boolean isInbound, String hostAndPort) {
			super(timestamp, status, reasonText);
			this.isInbound = isInbound;
			this.hostAndPort = hostAndPort;
		}
		
	}
	
	private static class StatusHolder {
		private final FastLinkedList<StatusEntry> m_statusTrail = new FastLinkedList<>(); // Keep the last X status entries
		
		public void addStatusTrail(StatusType status, String reasonText) {
			m_statusTrail.add(new StatusEntry(System.currentTimeMillis(), status, reasonText));
			while (m_statusTrail.count() > TEMPLATE_STATUS_TRAIL_SIZE) {
				m_statusTrail.removeFirst();
			}
		}
		
		protected StatusEntry getMostRecentStatus() {
			return m_statusTrail.peekLast();
		}
	}
	
	private static class TemplateStatusHolder extends StatusHolder{
		private final TemplateRef m_ref;
		
		TemplateStatusHolder(TemplateRef ref) {
			m_ref = ref;
		}
		
		CurrentStatus getCurrentStatus() {
			StatusEntry e = getMostRecentStatus();
			if (e == null) {
				return null;
			}
			return new CurrentTemplateStatus(e.timestamp, e.status, e.reasonText, m_ref);
		}
	}
	
	private static class NeuronStatusHolder extends StatusHolder{
		private final NeuronRef m_instanceRef;
		
		NeuronStatusHolder(NeuronRef instanceRef) {
			m_instanceRef = instanceRef;
		}
		
		CurrentStatus getCurrentStatus() {
			StatusEntry e = getMostRecentStatus();
			if (e == null) {
				return null;
			}
			return new CurrentNeuronStatus(e.timestamp, e.status, e.reasonText, m_instanceRef);
		}
	}
	
	private static class HostStatusHolder extends StatusHolder{
		private final boolean m_isInbound;
		private final String m_hostAndPort;
		
		HostStatusHolder(boolean isInbound, String hostAndPort) {
			m_isInbound = isInbound;
			m_hostAndPort = hostAndPort;
		}
		
		CurrentStatus getCurrentStatus() {
			StatusEntry e = getMostRecentStatus();
			if (e == null) {
				return null;
			}
			return new CurrentHostStatus(e.timestamp, e.status, e.reasonText, m_isInbound, m_hostAndPort);
		}
	}
	

	private static class StatusEntry extends FastLinkedList.LLNode<StatusEntry>{
		private final long timestamp;
		private final StatusType status;
		private final String reasonText;
		
		StatusEntry(long timestamp, StatusType status, String reasonText) {
			this.timestamp = timestamp;
			this.status = status;
			this.reasonText = reasonText;
		}

		@Override
		protected StatusEntry getObject() {
			return this;
		}
		
	}
	
	private static class Registrant implements INeuronApplicationSystem {

		@Override
		public String systemName()
		{
			return "StatusSystem";
		}
	}
}
