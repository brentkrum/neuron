package com.neuron.utility;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FixedSizeByteArrayTrie_Test {

	@Test
	public void testAddByteArray() {
		FixedSizeByteArrayTrie trie = new FixedSizeByteArrayTrie(3);
		
		byte[] key1 = new byte[] {1,2,3};
		Assertions.assertNull(trie.addOrFetch(key1, "value-1-0"));
		Assertions.assertEquals("value-1-0", trie.addOrFetch(key1, "value-1-1"));
		Assertions.assertEquals("value-1-0", trie.addOrFetch(key1, "value-1-2"));
		
		byte[] key2 = new byte[] {0,(byte)255,3};
		Assertions.assertNull(trie.addOrFetch(key2, "value-2-0"));
		Assertions.assertEquals("value-2-0", trie.addOrFetch(key2, "value-2-1"));
		Assertions.assertEquals("value-2-0", trie.addOrFetch(key2, "value-2-2"));

	}

	@Test
	public void testAddByteBuffer() {
		FixedSizeByteArrayTrie trie = new FixedSizeByteArrayTrie(3);
		
		ByteBuffer key1 = ByteBuffer.allocate(3);
		key1.put((byte)1).put((byte)2).put((byte)3);
		Assertions.assertNull(trie.addOrFetch(key1, 0, "value-1-0"));
		Assertions.assertEquals("value-1-0", trie.addOrFetch(key1, 0, "value-1-1"));
		Assertions.assertEquals("value-1-0", trie.addOrFetch(key1, 0, "value-1-2"));
		
		ByteBuffer key2 = ByteBuffer.allocate(3);
		key2.put((byte)0).put((byte)255).put((byte)3);
		Assertions.assertNull(trie.addOrFetch(key2, 0, "value-2-0"));
		Assertions.assertEquals("value-2-0", trie.addOrFetch(key2, 0, "value-2-1"));
		Assertions.assertEquals("value-2-0", trie.addOrFetch(key2, 0, "value-2-2"));

		try {
			trie.addOrFetch(key2, "value-3-0");
			Assertions.fail("Should not get here");
		} catch(UnsupportedOperationException ex) {
		}

		try {
			trie.addOrFetch(key2, 1, "value-4-0");
			Assertions.fail("Should not get here");
		} catch(UnsupportedOperationException ex) {
		}
	}
}
