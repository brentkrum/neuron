package com.neuron.utility;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IntTrie_Test {

	@Test
	public void test_AddOrFetch_Get_Simple() {
		IntTrie<Integer> trie = new IntTrie<Integer>();

		for(int key=0; key<4096; key++) {
			Integer value = Integer.valueOf(key);
			Assertions.assertNull(trie.addOrFetch(key, value));
			Assertions.assertEquals(value, trie.addOrFetch(key, Integer.valueOf(Integer.MAX_VALUE)));
			Assertions.assertEquals(value, trie.get(key));
		}
		for(int key=0; key<4096; key++) {
			Integer value = Integer.valueOf(key);
			Assertions.assertEquals(value, trie.addOrFetch(key, Integer.valueOf(Integer.MAX_VALUE)));
			Assertions.assertEquals(value, trie.get(key));
		}
	}
	
	@Test
	public void test_AddAndRemove_Simple() {
		IntTrie<Integer> trie = new IntTrie<Integer>();

		for(int key=0; key<4096; key++) {
			Integer value = Integer.valueOf(key);
			Assertions.assertNull(trie.addOrFetch(key, value));
		}
		for(int key=0; key<4096; key++) {
			Integer value = Integer.valueOf(key);
			Assertions.assertEquals(value, trie.remove(key));
		}
		for(int key=0; key<4096; key++) {
			Integer value = Integer.valueOf(key);
			Assertions.assertNull(trie.addOrFetch(key, value));
		}
		for(int key=4095; key>=0; key--) {
			Integer value = Integer.valueOf(key);
			Assertions.assertEquals(value, trie.remove(key));
		}
		Assertions.assertTrue(trie.isRootNodeEmpty(), "Root node is not empty");
	}
	
	@Test
	public void test_AddAndRemove_Interesting() {
		IntTrie<Integer> trie = new IntTrie<Integer>();

		Integer keys[] = {
				0xFF00_0000,
				0xFF00_00FF,
				0xFF00_FF00,
				0xFFFF_0000,
				0xFE00_0000,
				0xFE00_00FF,
				0xFE00_FF00,
				0xFEFF_0000,
		};
		for(int i=0; i<keys.length; i++) {
			int key = keys[i];
			Integer value = keys[i];
			Assertions.assertNull(trie.addOrFetch(key, value));
		}
		for(int i=0; i<keys.length; i++) {
			int key = keys[i];
			Integer value = keys[i];
			Assertions.assertEquals(value, trie.get(key));
		}
		for(int i=0; i<keys.length; i++) {
			int key = keys[i];
			Integer value = keys[i];
			Assertions.assertEquals(value, trie.remove(key));
		}
		for(int i=0; i<keys.length; i++) {
			int key = keys[i];
			Integer value = keys[i];
			Assertions.assertNull(trie.addOrFetch(key, value));
		}
		for(int i=keys.length-1; i>=0; i--) {
			int key = keys[i];
			Integer value = keys[i];
			Assertions.assertEquals(value, trie.remove(key));
		}
		Assertions.assertTrue(trie.isRootNodeEmpty(), "Root node is not empty");
	}
	
	@Test
	public void test_AddOrReplace_Simple() {
		IntTrie<String> trie = new IntTrie<>();

		Assertions.assertNull(trie.addOrReplace(1, "a"));
		Assertions.assertEquals("a", trie.addOrReplace(1, "b"));
		Assertions.assertEquals("b", trie.get(1));
		Assertions.assertNull(trie.addOrReplace(2, "a"));
	}
}
