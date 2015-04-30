package relop;


/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by AND operators.
 */
public class Selection extends Iterator {

	private Iterator iterator;
	private Predicate[] preds;
	private	 Tuple currentTuple;
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
	  this.iterator = iter;
	  this.preds = preds.clone();
	  currentTuple = null;
   // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("Select * From "+iterator.toString());
	  iterator.explain(depth+1);
	  System.out.print("Where ");
	  for(int i = 0; i< preds.length;i++)
		  System.out.print(preds[i].toString()+((i  == preds.length -1)?"\n":" "));
   // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  iterator.restart();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return iterator.isOpen();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  currentTuple = null;
	 iterator.close();
   // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if(currentTuple == null){
		  while(iterator.hasNext()){
			  Tuple canTuple = iterator.getNext();
			  boolean valid = true;
			  for(int i = 0; i< preds.length;i++)
				  valid &= preds[i].evaluate(canTuple);
			  if(valid){
				  currentTuple = canTuple;
				  return true;
			  }
		  }
		  return false;
	  }
	  return true;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  if(isOpen() && hasNext()){
		  Tuple tuple = currentTuple;
		  currentTuple = null;
		  return tuple;
	  }
    throw new IllegalStateException("No More Tuples.");
  }

} // public class Selection extends Iterator
