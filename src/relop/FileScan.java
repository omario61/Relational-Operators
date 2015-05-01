package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

	
	//private Schema schema ;
	private HeapFile file;
	private HeapScan iterator;
	private boolean isOpen;
	private RID lastRID;
	
  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
	 this.setSchema(schema);
	  // this.schema=schema;
	  this.file=file;
	  iterator=file.openScan();
	  isOpen=true;
	  lastRID=null;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("File  Scan "+file.toString());
	  //  throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  if(isOpen()){
		  close();
	  }
	  iterator=file.openScan();
	  isOpen=true;
	  lastRID=null;
	  
   // throw new UnsupportedOperationException("Not implemented");
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
	  iterator.close();//Closes the file scan, releasing any pinned pages.
	  isOpen=false;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if(isOpen()){
		  return iterator.hasNext();
	  }
	return false;
	  //  throw new IllegalStateException("File is closed.");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  
	  
	  if(hasNext()){
		  RID rid = new RID();;//output parameter
		  lastRID=rid;
		  byte[]data=iterator.getNext(rid);
		  Tuple output=new Tuple(getSchema(), data);
		  return output;
	  }
	  throw new IllegalStateException("No More Tuples.");
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
	  return lastRID;
   // throw new UnsupportedOperationException("Not implemented");
  }

} // public class FileScan extends Iterator
