package org.apache.mahout.cf.taste.impl.model.cassandra;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;


  public class ColumnNameIterator implements LongPrimitiveIterator {
	private Long peeked = null;
	private ColumnSliceIterator iterator;
	public ColumnNameIterator(ColumnSliceIterator<Long, Long, byte[]> sliceIt) {
		this.iterator = sliceIt;
	}
	@Override
	public boolean hasNext() {
		return iterator.hasNext() || peeked != null;
	}
	@Override
	public Long next() {
		HColumn<Long, byte[]> column = iterator.next();
		if(column == null)
			return null;
		return column.getName();
	}
	@Override
	public long nextLong() {
		if(peeked != null) {
			Long p = peeked;
			peeked = null;
			return p;
		}
		return next();
	}
	@Override
	public long peek() {
		if(peeked == null) 
			peeked = next();
		return peeked;
	}

	@Override
	public void skip(int n) {
		for(int i=0;i<n;i++) {
			if(!hasNext())
				break;
			next();
		}
	}
	@Override
	public void remove() {
		skip(1);
	}
  }
