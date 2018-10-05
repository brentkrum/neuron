package com.neuron.utility;

import java.util.LinkedList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CharSequenceTrie_Test {

	@Test
	public void test_AddOrFetch_Get_Simple() {
		CharSequenceTrie<String> trie = new CharSequenceTrie<>();
		LinkedList<String> addedValues = new LinkedList<>();
		
		addedValues.add( test_AddOrFetch_Get_Simple_ADD(trie, "a") );
		test_AddOrFetch_Get_Simple_VERIFY(trie, addedValues);

		addedValues.add( test_AddOrFetch_Get_Simple_ADD(trie, "b") );
		test_AddOrFetch_Get_Simple_VERIFY(trie, addedValues);

		addedValues.add( test_AddOrFetch_Get_Simple_ADD(trie, "ab") );
		test_AddOrFetch_Get_Simple_VERIFY(trie, addedValues);
		
		addedValues.add( test_AddOrFetch_Get_Simple_ADD(trie, "ac") );
		test_AddOrFetch_Get_Simple_VERIFY(trie, addedValues);
	}
	
	private String test_AddOrFetch_Get_Simple_ADD(CharSequenceTrie<String> trie, String s) {
		Assertions.assertNull(trie.addOrFetch(s, s));
		return s;
	}
	private void test_AddOrFetch_Get_Simple_VERIFY(CharSequenceTrie<String> trie, LinkedList<String> addedValues) {
		for(String s : addedValues) {
			Assertions.assertEquals(s, trie.addOrFetch(s, "BLAH"));
			Assertions.assertEquals(s, trie.get(s));
		}
	}

	@Test
	public void test_Remove_Simple() {
		CharSequenceTrie<String> trie = new CharSequenceTrie<>();
		
		test_Remove_Simple_ADD(trie, "a");
		test_Remove_Simple_ADD(trie, "b");
		test_Remove_Simple_ADD(trie, "ab");
		test_Remove_Simple_ADD(trie, "ac");
		
		trie.remove("a");
		trie.remove("b");
		trie.remove("ab");
		trie.remove("ac");
		
		Assertions.assertTrue(trie.isRootNodeEmpty(), "Root node is not empty");
	}

	@Test
	public void test_Replace_Simple() {
		CharSequenceTrie<String> trie = new CharSequenceTrie<>();
		
		trie.addOrFetch("a", "a");
		Assertions.assertEquals("a", trie.replace("a", "b"));
		Assertions.assertEquals("b", trie.get("a"));
		
		Assertions.assertNull(trie.replace("apple", "c"));
		Assertions.assertEquals("b", trie.get("a"));
	}
	
	void test_Remove_Simple_ADD(CharSequenceTrie<String> trie, String s) {
		trie.addOrFetch(s, s);
	}

	@Test
	public void test_ForEach_Simple() {
		CharSequenceTrie<String> trie = new CharSequenceTrie<>();
		
		trie.addOrFetch("ddd", "ddd");
		trie.addOrFetch("a", "a");
		trie.addOrFetch("b", "b");
		trie.addOrFetch("c", "c");
		trie.addOrFetch("d", "d");
		trie.addOrFetch("aa", "aa");
		trie.addOrFetch("bb", "bb");
		trie.addOrFetch("cc", "cc");
		trie.addOrFetch("dd", "dd");
		
		LinkedList<String> found = new LinkedList<>();
		trie.forEach((key, value) -> {
			Assertions.assertEquals(key, value);
			found.add(key.toString());
			System.out.println(key);
			return true;
		});
		
		for(String key : found) {
			trie.remove(key);
		}
		Assertions.assertTrue(trie.isRootNodeEmpty(), "Root node is not empty");
	}
}
