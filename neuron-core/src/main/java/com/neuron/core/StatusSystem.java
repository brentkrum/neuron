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
	private static final int HOST_STATUS_TRAIL_SIZE = Config.getFWInt("core.StatusSystem.hostStatusTrailSize", 16);
	private static final int NEURON_STATUS_TRAIL_SIZE = Config.getFWInt("core.StatusSystem.neuronStatusTrailSize", 16);
	private static final int TEMPLATE_STATUS_TRAIL_SIZE = Config.getFWInt("core.StatusSystem.templateStatusTrailSize", 16);
	private static final int GROUP_STATUS_TRAIL_SIZE = Config.getFWInt("core.StatusSystem.groupStatusTrailSize", 16);
	
	private static final Object m_groupLock = new Object();
	private static final IntTrie<GroupStatusHolder> m_groupStatusTrie = new IntTrie<>();
	
	private static final Object m_templateLock = new Object();
	private static final IntTrie<TemplateStatusHolder> m_templateStatusTrie = new IntTrie<>();
	
	private static final Object m_neuronLock = new Object();
	private static final IntTrie<NeuronStatusHolder> m_neuronStatusTrie = new IntTrie<>();
	
	private static final Object m_hostLock = new Object();
	private static final CharSequenceTrie<HostStatusHolder> m_hostStatusTrie = new CharSequenceTrie<>();

	static void register() {
		NeuronApplication.register(new Registrant());
	}

	public static void setStatus(GroupRef ref, StatusType status, String reasonText) {
		synchronized(m_groupLock) {
			GroupStatusHolder h = m_groupStatusTrie.get(ref.id());
			if (h == null) {
				h = new GroupStatusHolder(ref);
				m_groupStatusTrie.addOrFetch(ref.id(), h);
			}
			h.addStatusTrail(ref, status, reasonText);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: {} {}", ref.logString(), status, reasonText);
		}
	}
	
	public static void setStatus(TemplateRef ref, StatusType status, String reasonText) {
		synchronized(m_templateLock) {
			TemplateStatusHolder h = m_templateStatusTrie.get(ref.id());
			if (h == null) {
				h = new TemplateStatusHolder(ref);
				m_templateStatusTrie.addOrFetch(ref.id(), h);
			}
			h.addStatusTrail(ref, status, reasonText);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: {} {}", ref.logString(), status, reasonText);
		}
	}
	
	public static void setStatus(NeuronRef ref, StatusType status, String reasonText) {
		synchronized(m_neuronLock) {
			NeuronStatusHolder h = m_neuronStatusTrie.get(ref.id());
			if (h == null) {
				h = new NeuronStatusHolder(ref);
				m_neuronStatusTrie.addOrFetch(ref.id(), h);
			}
			h.addStatusTrail(ref, status, reasonText);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("({}: {} {}", ref.logString(), status, reasonText);
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
		synchronized(m_groupLock) {
			m_groupStatusTrie.forEach((key, h) -> {
				CurrentStatus cs = h.getCurrentStatus();
				if (cs != null) {
					out.add(cs);
				}
				return true;
			});
		}
		synchronized(m_templateLock) {
			m_templateStatusTrie.forEach((key, h) -> {
				CurrentStatus cs = h.getCurrentStatus();
				if (cs != null) {
					out.add(cs);
				}
				return true;
			});
		}
		synchronized(m_neuronLock) {
			m_neuronStatusTrie.forEach((key, h) -> {
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
	
	public static class CurrentGroupStatus extends CurrentStatus {
		public final GroupRef groupRef;
		
		CurrentGroupStatus(long timestamp, StatusType status, String reasonText, GroupRef ref) {
			super(timestamp, status, reasonText);
			this.groupRef = ref;
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
		
		protected void _addStatusTrail(int maxTrailSize, StatusEntry entry) {
			m_statusTrail.add(entry);
			while (m_statusTrail.count() > maxTrailSize) {
				m_statusTrail.removeFirst();
			}
		}
		
		protected StatusEntry getMostRecentStatus() {
			return m_statusTrail.peekLast();
		}
	}
	
	@SuppressWarnings("unused")
	private static class GroupStatusHolder extends StatusHolder{
		private final int m_id;
		private final String m_name;
		
		GroupStatusHolder(GroupRef ref) {
			m_id = ref.id();
			m_name = ref.name();
		}
		
		CurrentStatus getCurrentStatus() {
			GroupStatusEntry e = (GroupStatusEntry)getMostRecentStatus();
			if (e == null) {
				return null;
			}
			return new CurrentGroupStatus(e.timestamp, e.status, e.reasonText, e.m_ref);
		}
		
		public void addStatusTrail(GroupRef ref, StatusType status, String reasonText) {
			_addStatusTrail(GROUP_STATUS_TRAIL_SIZE, new GroupStatusEntry(System.currentTimeMillis(), status, reasonText, ref));
		}
		
		private static class GroupStatusEntry extends StatusEntry {
			private final GroupRef m_ref;
			
			GroupStatusEntry(long timestamp, StatusType status, String reasonText, GroupRef ref) {
				super(timestamp, status, reasonText);
				this.m_ref = ref;
			}
			
		}
	}
	
	@SuppressWarnings("unused")
	private static class TemplateStatusHolder extends StatusHolder{
		private final int m_id;
		private final String m_name;
		
		TemplateStatusHolder(TemplateRef ref) {
			m_id = ref.id();
			m_name = ref.name();
		}
		
		CurrentStatus getCurrentStatus() {
			TemplateStatusEntry e = (TemplateStatusEntry)getMostRecentStatus();
			if (e == null) {
				return null;
			}
			return new CurrentTemplateStatus(e.timestamp, e.status, e.reasonText, e.m_ref);
		}
		
		public void addStatusTrail(TemplateRef ref, StatusType status, String reasonText) {
			_addStatusTrail(TEMPLATE_STATUS_TRAIL_SIZE, new TemplateStatusEntry(System.currentTimeMillis(), status, reasonText, ref));
		}
		
		private static class TemplateStatusEntry extends StatusEntry {
			private final TemplateRef m_ref;
			
			TemplateStatusEntry(long timestamp, StatusType status, String reasonText, TemplateRef ref) {
				super(timestamp, status, reasonText);
				this.m_ref = ref;
			}
			
		}
	}
	
	@SuppressWarnings("unused")
	private static class NeuronStatusHolder extends StatusHolder{
		private final int m_id;
		private final String m_name;
		
		NeuronStatusHolder(NeuronRef ref) {
			m_id = ref.id();
			m_name = ref.name();
		}
		
		CurrentStatus getCurrentStatus() {
			NeuronStatusEntry e = (NeuronStatusEntry)getMostRecentStatus();
			if (e == null) {
				return null;
			}
			return new CurrentNeuronStatus(e.timestamp, e.status, e.reasonText, e.m_ref);
		}
		
		public void addStatusTrail(NeuronRef ref, StatusType status, String reasonText) {
			_addStatusTrail(NEURON_STATUS_TRAIL_SIZE, new NeuronStatusEntry(System.currentTimeMillis(), status, reasonText, ref));
		}
		
		private static class NeuronStatusEntry extends StatusEntry {
			private final NeuronRef m_ref;
			
			NeuronStatusEntry(long timestamp, StatusType status, String reasonText, NeuronRef ref) {
				super(timestamp, status, reasonText);
				this.m_ref = ref;
			}
			
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
		
		public void addStatusTrail(StatusType status, String reasonText) {
			_addStatusTrail(HOST_STATUS_TRAIL_SIZE, new StatusEntry(System.currentTimeMillis(), status, reasonText));
		}
	}
	

	private static class StatusEntry extends FastLinkedList.LLNode<StatusEntry> {
		protected final long timestamp;
		protected final StatusType status;
		protected final String reasonText;
		
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
