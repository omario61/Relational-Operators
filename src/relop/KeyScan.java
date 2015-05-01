package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

	//private Schema schema;
	private HashIndex index;
	private SearchKey key;
	private HeapFile file;
	private HashScan iterator;
	private boolean isOpen;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
	  this.setSchema(schema);
	  //this.schema=schema;
	  this.index=index;
	  this.key=key;
	  this.file=file;
	  iterator=index.openScan(key);
	  isOpen=true;
	  //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("Aywa  e3ni  3awz eh ^_^");

	  // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  
	  close();
	  iterator=index.openScan(key);
	  isOpen=true;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return isOpen;
	  //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  if(isOpen()){
		  iterator.close();
		  isOpen=false;
	  }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    if(isOpen()){
    	System.out.println("^__________^ "+iterator.hasNext());
    	//iterator.hasNext();
    	return iterator.hasNext();
    }
	throw new IllegalStateException("Scanner is closed");
	  
	  //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
		  System.out.println("^_^_^_");
		  System.out.println(hasNext());
	  
	  if(hasNext()){
		  RID rid=iterator.getNext();
		  byte []data=file.selectRecord(rid);
		  return new Tuple(getSchema(), data) ;
	  }
		throw new IllegalStateException("No more tubles");
	  
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class KeyScan extends Iterator
