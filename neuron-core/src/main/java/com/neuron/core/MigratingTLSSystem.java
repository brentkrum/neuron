package com.neuron.core;


public class MigratingTLSSystem
{
//	private static final ReadWriteLock m_rwLock = new ReentrantReadWriteLock();
//	private static int m_nextIndex;
//	private static boolean m_zeroMigrators = true;
//	private static ArrayList<MigrationSlot> m_migrators = new ArrayList<>();
//	private static MyFastThreadLocal m_tls = new MyFastThreadLocal();
//	private static ArrayList<TLSData> m_dataPool = new ArrayList<>(32);
//	
//	static void register() {
//		NeuronApplication.register(new Registrant());
//	}
//	
//	public static int nextIndex() {
//		m_rwLock.writeLock().lock();
//		try {
//			return m_nextIndex++;
//		} finally {
//			m_rwLock.writeLock().unlock();
//		}
//	}
//	
//	@SuppressWarnings("unchecked")
//	public static <V> void registerMigrator(int index, IMigrator<V> migrator) {
//		m_rwLock.writeLock().lock();
//		try {
//			m_zeroMigrators = false;
//			m_migrators.add(new MigrationSlot(index, (IMigrator<Object>) migrator));
//		} finally {
//			m_rwLock.writeLock().unlock();
//		}
//	}
//	
//	public static <V> V get(int slotIndex) {
//		return m_tls.get().data.get(slotIndex);
//	}
//	
//	public static void put(int slotIndex, Object o) {
//		m_tls.get().data.put(slotIndex, o);
//	}
//	
//	/*
//	 * 
//	 * Need to stop doing it this way.  If we share the same TLSData then when each thread
//	 * does a put, we are putting into the same objects!!!
//	 * 
//	 * Need to just arraycopy migrated data into a new array, then retain the ref counted ones
//	 * 
//	 * Also, need to clean up every time we return an array back to the pool 
//	 * 
//	 * 
//	 */
//	
//	
//	public static IMigratingTLSData createMigrationTLSData() {
//		if (m_zeroMigrators) {
//			TLSData tlsData = m_tls.get().data;
//			if (tlsData.m_hasRefCountedObjects) {
//			final Object[] slots = holder.data.m_slots;
//			for(int i=0; i<slots.length; i++) {
//				ReferenceCountUtil.retain(slots[i]);
//			}
//			return (IMigratingTLSData)m_tls.get().retain();
//		}
//		final TLSDataHolder holder = getNewTLSDataHolder();
//		final Object[] oldSlots = m_tls.get().data.m_slots;
//		final Object[] newSlots = holder.data.m_slots;
//		System.arraycopy(oldSlots, 0, newSlots, 0, oldSlots.length);
//		m_rwLock.readLock().lock();
//		try {
//			for(int i=0; i<m_migrators.size(); i++) {
//				final MigrationSlot ms = m_migrators.get(i);
//				newSlots[ms.index] = ms.migrator.migrate(newSlots[ms.index]);
//			}
//		} finally {
//			m_rwLock.readLock().unlock();
//		}
//		
//		return holder;
//	}
//
//	public static void releaseMigrationTLSData(IMigratingTLSData migration) {
//		((TLSDataHolder)migration).release();
//	}
//	
//	public static void pushTLSData(IMigratingTLSData migration) {
//		TLSDataHolder holder = (TLSDataHolder)migration;
//		if (m_tls.isSet()) {
//			holder.prev = m_tls.get();
//		}
//		m_tls.set(holder);
//	}
//	
//	public static void popTLSData() {
//		TLSDataHolder holder = m_tls.get();
//		if (holder.prev != null) {
//			m_tls.set(holder.prev);
//		} else {
//			m_tls.set(null);
//			holder.release();
//		}
//	}
//
//	public interface IMigratingTLSData {
//	}
//	
//	public interface IMigrator<V> {
//		V migrate(V object);
//	}
//	
//	private static TLSDataHolder getNewTLSDataHolder() {
//		TLSData tlsData = null;
//		m_rwLock.writeLock().lock();
//		try {
//			if (m_dataPool.size() != 0) {
//				tlsData = m_dataPool.remove(m_dataPool.size()-1);
//			}
//		} finally {
//			m_rwLock.writeLock().unlock();
//		}
//		if (tlsData == null) {
//			tlsData = new TLSData();
//		}
//		return new TLSDataHolder(tlsData);
//	}
//	
//	private static class Registrant implements INeuronApplicationSystem {
//
//		@Override
//		public String systemName()
//		{
//			return "TimerSystem";
//		}
//	}
//	
//	private static class TLSDataHolder extends AbstractReferenceCounted implements IMigratingTLSData {
//		private final TLSData data;
//		private TLSDataHolder prev;
//		
//		TLSDataHolder(TLSData data) {
//			this.data = data;
//		}
//		
//		@Override
//		public ReferenceCounted touch(Object hint)
//		{
//			return this;
//		}
//
//		@Override
//		protected void deallocate()
//		{
//			m_rwLock.writeLock().lock();
//			try {
//				m_dataPool.add(this.data);
//			} finally {
//				m_rwLock.writeLock().unlock();
//			}
//		}
//		
//	}
//	
//	private static class TLSData {
//		private Object[] m_slots = new Object[m_nextIndex];
//		private boolean m_hasRefCountedObjects;
//		
//		@SuppressWarnings("unchecked")
//		public <V> V get(int slotIndex) {
//			if (slotIndex >= m_slots.length) {
//				return null;
//			}
//			return (V)m_slots[slotIndex];
//		}
//		
//		public void put(int slotIndex, Object o) {
//			if (slotIndex >= m_slots.length) {
//				m_slots = Arrays.copyOf(m_slots, m_slots.length*2);
//			}
//			m_slots[slotIndex] = o;
//			m_hasRefCountedObjects |= o instanceof ReferenceCounted;
//		}
//	}
//
//	private static class MigrationSlot {
//		int index;
//		IMigrator<Object> migrator;
//		
//		MigrationSlot(int index, IMigrator<Object> migrator) {
//			this.index = index;
//			this.migrator = migrator;
//		}
//	}
//	
//	private static class MyFastThreadLocal extends FastThreadLocal<TLSDataHolder> {
//
//		@Override
//		protected TLSDataHolder initialValue() throws Exception
//		{
//			return getNewTLSDataHolder();
//		}
//
//		@Override
//		protected void onRemoval(TLSDataHolder holder) throws Exception
//		{
//			if (holder != null) {
//				holder.release();
//			}
//		}
//		
//	}
}
