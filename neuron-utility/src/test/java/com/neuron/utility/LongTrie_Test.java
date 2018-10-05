package com.neuron.utility;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LongTrie_Test {

	@Test
	public void test_AddOrFetch_Get_Simple() {
		LongTrie<Long> trie = new LongTrie<Long>();

		for(int key=0; key<4096; key++) {
			Long value = Long.valueOf(key);
			Assertions.assertNull(trie.addOrFetch(key, value));
			Assertions.assertEquals(value, trie.addOrFetch(key, Long.valueOf(Long.MAX_VALUE)));
			Assertions.assertEquals(value, trie.get(key));
		}
		for(int key=0; key<4096; key++) {
			Long value = Long.valueOf(key);
			Assertions.assertEquals(value, trie.addOrFetch(key, Long.valueOf(Long.MAX_VALUE)));
			Assertions.assertEquals(value, trie.get(key));
		}
	}
	
	@Test
	public void test_AddAndRemove_Simple() {
		LongTrie<Long> trie = new LongTrie<>();

		for(int key=0; key<4096; key++) {
			Long value = Long.valueOf(key);
			Assertions.assertNull(trie.addOrFetch(key, value));
		}
		for(int key=0; key<4096; key++) {
			Long value = Long.valueOf(key);
			Assertions.assertEquals(value, trie.remove(key));
		}
		for(int key=0; key<4096; key++) {
			Long value = Long.valueOf(key);
			Assertions.assertNull(trie.addOrFetch(key, value));
		}
		for(int key=4095; key>=0; key--) {
			Long value = Long.valueOf(key);
			Assertions.assertEquals(value, trie.remove(key));
		}
		Assertions.assertTrue(trie.isRootNodeEmpty(), "Root node is not empty");
	}
	
	@Test
	public void test_AddAndRemove_Interesting() {
		LongTrie<Long> trie = new LongTrie<Long>();

		Long keys[] = {
				0xFF00_0000_0000_0000l,
				0xFF00_0000_0000_00FFl,
				0xFF00_0000_0000_FF00l,
				0xFF00_0000_00FF_0000l,
				0xFF00_0000_FF00_0000l,
				0xFF00_00FF_0000_0000l,
				0xFF00_FF00_0000_0000l,
				0xFFFF_0000_0000_0000l,
				0xFE00_0000_0000_0000l,
				0xFE00_0000_0000_00FFl,
				0xFE00_0000_0000_FF00l,
				0xFE00_0000_00FF_0000l,
				0xFE00_0000_FF00_0000l,
				0xFE00_00FF_0000_0000l,
				0xFE00_FF00_0000_0000l,
				0xFEFF_0000_0000_0000l,
		};
		for(int i=0; i<keys.length; i++) {
			long key = keys[i];
			Long value = keys[i];
			Assertions.assertNull(trie.addOrFetch(key, value));
		}
		for(int i=0; i<keys.length; i++) {
			long key = keys[i];
			Long value = keys[i];
			Assertions.assertEquals(value, trie.get(key));
		}
		for(int i=0; i<keys.length; i++) {
			long key = keys[i];
			Long value = keys[i];
			Assertions.assertEquals(value, trie.remove(key));
		}
		for(int i=0; i<keys.length; i++) {
			long key = keys[i];
			Long value = keys[i];
			Assertions.assertNull(trie.addOrFetch(key, value));
		}
		for(int i=keys.length-1; i>=0; i--) {
			long key = keys[i];
			Long value = keys[i];
			Assertions.assertEquals(value, trie.remove(key));
		}
		Assertions.assertTrue(trie.isRootNodeEmpty(), "Root node is not empty");
	}
}
