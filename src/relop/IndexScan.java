package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
	private HashIndex index;
	private HeapFile file;
	private BucketScan iterator;
	private boolean isClosed;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  setSchema(schema);
	  this.index = index;
	  this.file = file;
	  iterator = index.openScan();
	  isClosed = false;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("Index  Scan "+index.toString());
   // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  if(this.isOpen())
		  iterator.close();
	  iterator = index.openScan();
	  isClosed = false;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return !isClosed;
  //  throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  if(this.isOpen()){
		  iterator.close();
		//  iterator = null; //not sure about it.
	  }
	 isClosed = true;
   // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if(isOpen())
		  return iterator.hasNext();
	  return false;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  if(this.isOpen()&&iterator.hasNext()){
		  RID rid = iterator.getNext();
		  byte [] data = file.selectRecord(rid);
		  Tuple tuple = new Tuple(getSchema(), data);
		  return tuple;
	  }
	  throw new IllegalStateException("No More Tuples.");
  //  throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
		  return iterator.getLastKey();
  // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
		  return iterator.getNextHash();
  //  throw new UnsupportedOperationException("Not implemented");
  }

} // public class IndexScan extends Iterator
