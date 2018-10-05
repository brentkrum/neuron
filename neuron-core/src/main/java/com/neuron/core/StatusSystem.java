package com.neuron.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.FastLinkedList;
import com.neuron.utility.IntTrie;

public class StatusSystem
{
	private static final Logger LOG = LogManager.getLogger(StatusSystem.class);
	private static final int TEMPLATE_STATUS_TRAIL_SIZE = Config.getFWInt("core.StatusSystem.templateStatusTrailSize", 16);
	private static final String NEURON_TEMPLATE_TYPE_NAME = "NeuronTemplate";
	private static final String NEURON_INSTANCE_TYPE_NAME = "Neuron";
	
	private static final ReadWriteLock m_typeLock = new ReentrantReadWriteLock(true);
	private static final CharSequenceTrie<String> m_types = new CharSequenceTrie<>();
	private static final List<String> m_typesList = new LinkedList<>();
	
	private static final Object m_templateLock = new Object();
	private static final IntTrie<TemplateStatusHolder> m_templateStatusTrie = new IntTrie<>();
	
	private static final Object m_neuronLock = new Object();
	private static final IntTrie<NeuronStatusHolder> m_neuronStatusTrie = new IntTrie<>();
	
	static {
		m_types.addOrFetch(NEURON_TEMPLATE_TYPE_NAME, "");
		m_typesList.add(NEURON_TEMPLATE_TYPE_NAME);
		m_types.addOrFetch(NEURON_INSTANCE_TYPE_NAME, "");
		m_typesList.add(NEURON_INSTANCE_TYPE_NAME);
	}

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
	public static void setStatus(TemplateRef ref, String status) {
		synchronized(m_templateLock) {
			TemplateStatusHolder h = m_templateStatusTrie.get(ref.generation());
			if (h == null) {
				h = new TemplateStatusHolder(ref);
				m_templateStatusTrie.addOrFetch(ref.generation(), h);
			}
			h.addStatusTrail(status);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: {}", ref.logString(), status);
		}
	}
	
	public static void setStatus(NeuronRef instanceRef, String status) {
		synchronized(m_neuronLock) {
			NeuronStatusHolder h = m_neuronStatusTrie.get(instanceRef.generation());
			if (h == null) {
				h = new NeuronStatusHolder(instanceRef);
				m_neuronStatusTrie.addOrFetch(instanceRef.generation(), h);
			}
			h.addStatusTrail(status);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("({}) {}: {}", instanceRef.templateRef().logString(), instanceRef.logString(), status);
		}
	}

	public static List<String> getStatusTypes() {
		final List<String> out;
		m_typeLock.readLock().lock();
		try {
			out = new ArrayList<String>(m_typesList.size());
			for(String s : m_typesList) {
				out.add(s);
			}
		} finally {
			m_typeLock.readLock().unlock();
		}
		return out;
	}
	
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
		return out;
	}
	
	public static class CurrentStatus {
		public final String type;
		public final TemplateRef templateRef;
		public final NeuronRef neuronRef;
		public final long timestamp;
		public final String status;
		
		CurrentStatus(NeuronRef ref, long timestamp, String status) {
			this.type = NEURON_TEMPLATE_TYPE_NAME;
			this.timestamp = timestamp;
			this.status = status;
			this.templateRef = ref.templateRef();
			this.neuronRef = ref;
		}
		
		CurrentStatus(TemplateRef ref, long timestamp, String status) {
			this.type = NEURON_TEMPLATE_TYPE_NAME;
			this.timestamp = timestamp;
			this.status = status;
			this.templateRef = ref;
			this.neuronRef = null;
		}
	}
	
	private static class StatusHolder {
		private final FastLinkedList<StatusEntry> m_statusTrail = new FastLinkedList<>(); // Keep the last X status entries
		
		public void addStatusTrail(String status) {
			m_statusTrail.add(new StatusEntry(System.currentTimeMillis(), status));
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
			return new CurrentStatus(m_ref, e.timestamp, e.status);
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
			return new CurrentStatus(m_instanceRef, e.timestamp, e.status);
		}
	}
	

	private static class StatusEntry extends FastLinkedList.LLNode<StatusEntry> {
		private final long timestamp;
		private final String status;
		
		StatusEntry(long timestamp, String status) {
			this.timestamp = timestamp;
			this.status = status;
		}
		
		@Override
		protected StatusEntry getObject()
		{
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
