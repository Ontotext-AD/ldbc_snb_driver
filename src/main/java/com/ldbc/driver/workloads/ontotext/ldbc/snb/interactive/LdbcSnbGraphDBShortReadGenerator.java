package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Queues;

public class LdbcSnbGraphDBShortReadGenerator {

	static Queue<Long> synchronizedCircularQueueBuffer(int bufferSize) {
		return Queues.synchronizedQueue(EvictingQueue.<Long>create(bufferSize));
	}

	static Queue<Long> constantBuffer(final long value) {
		return new Queue<Long>() {
			@Override
			public boolean add(Long aLong) {
				return true;
			}

			@Override
			public boolean offer(Long aLong) {
				return true;
			}

			@Override
			public Long remove() {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public Long poll() {
				return value;
			}

			@Override
			public Long element() {
				return value;
			}

			@Override
			public Long peek() {
				return value;
			}

			@Override
			public int size() {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public boolean isEmpty() {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public boolean contains(Object o) {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public Iterator<Long> iterator() {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public Object[] toArray() {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public <T> T[] toArray(T[] a) {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public boolean remove(Object o) {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public boolean containsAll(Collection<?> c) {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public boolean addAll(Collection<? extends Long> c) {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public boolean removeAll(Collection<?> c) {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public boolean retainAll(Collection<?> c) {
				throw new UnsupportedOperationException("Method not implemented");
			}

			@Override
			public void clear() {
				throw new UnsupportedOperationException("Method not implemented");
			}
		};
	}
}
