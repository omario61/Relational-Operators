package relop;

import java.util.ArrayList;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

	private ArrayList<Tuple> result;
	private int index;
	private Iterator left;
	private Iterator right;
	private Predicate conditions[];
	private boolean  open;
	
	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
		result = new ArrayList<Tuple>();
		index = 0;
		this.left=left;
		this.right=right;
		conditions=preds;
		setSchema(Schema.join(this.left.getSchema(),this.right.getSchema()));
		join(left, right, preds);
		open=true;
		// throw new UnsupportedOperationException("Not implemented");
	}

	private void join(Iterator left, Iterator right, Predicate[] conditions) {
		while (left.hasNext()) {
			Tuple t1 = left.getNext();
			while (right.hasNext()) {
				Tuple t2 = right.getNext();
				Schema combinedSchema = Schema.join(left.getSchema(),
						right.getSchema());
				Tuple combinedTuple = Tuple.join(t1, t2, combinedSchema);
				boolean satisfy_condtion = true;
				for (int condition = 0; condition < conditions.length; condition++) {
					if (conditions[condition].evaluate(combinedTuple) == false) {
						satisfy_condtion = false;
						break;
					}
				}
				if (satisfy_condtion) {
					result.add(combinedTuple);
				}

			}
			right.restart();
		}
		left.close();
		right.close();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		System.out.println("Aywa e3ni 3awz eh ^__^");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		if(isOpen()){
			index=0;
		}else {
			result = new ArrayList<Tuple>();
			index = 0;
			join(left, right, conditions);
		}
		//throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
	return open;
		//	throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		open=false;
		result=null;
		//throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (index >= result.size())
			return false;
		return true;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (hasNext()) {
			return result.get(index++);
		}
		throw new IllegalStateException("No more tubles");
	}

} // public class SimpleJoin extends Iterator
