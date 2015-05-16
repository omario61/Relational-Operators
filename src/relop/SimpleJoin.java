package relop;

import java.util.ArrayList;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

	private ArrayList<Tuple> result;
	// private int index;
	private Iterator outer;
	private Iterator inner;
	private Predicate conditions[];
	private Predicate conditions2[][];
	private boolean lastResultComsumed;
	private Tuple current_outer;
	private Tuple current_result;
	private int MODE;// 1 means constructor 1 , 2means constructor 2
	private boolean open;

	/**
	 * Constructs a join, given the outer and inner iterators and join
	 * predicates (relative to the combined schema).
	 */
	public SimpleJoin(Iterator outer, Iterator inner, Predicate... preds) {
		MODE = 1;
		// result = new ArrayList<Tuple>();
		// index = 0;
		this.outer = outer;
		this.inner = inner;
		conditions = preds;

		setSchema(Schema.join(this.outer.getSchema(), this.inner.getSchema()));
		// join(outer, inner, preds);
		open = true;
		lastResultComsumed = true;
		if (outer.hasNext()) {
			current_outer = outer.getNext();
		}
		// throw new UnsupportedOperationException("Not implemented");
	}

	public SimpleJoin(Iterator outer, Iterator inner, Predicate[][] preds) {
		MODE = 2;
		// result = new ArrayList<Tuple>();
		// index = 0;
		this.outer = outer;
		this.inner = inner;
		conditions2 = preds;
		setSchema(Schema.join(this.outer.getSchema(), this.inner.getSchema()));
		// join(outer, inner, preds);
		open = true;
		lastResultComsumed = true;
		if (outer.hasNext()) {
			current_outer = outer.getNext();
		}
		// throw new UnsupportedOperationException("Not implemented");
	}

	private void join(Iterator outer, Iterator inner, Predicate[][] conditions) {
		while (outer.hasNext()) {
			Tuple t1 = outer.getNext();
			while (inner.hasNext()) {
				Tuple t2 = inner.getNext();
				Schema combinedSchema = Schema.join(outer.getSchema(),
						inner.getSchema());
				Tuple combinedTuple = Tuple.join(t1, t2, combinedSchema);
				boolean satisfy_condtion = true;
				for (int i = 0; i < conditions2.length; i++) {
					boolean b = false;
					for (int j = 0; j < conditions2[i].length; j++) {
						if (conditions[i][j].evaluate(combinedTuple) == true) {
							b = true;
							break;
						}

					}
					if (b == false) {
						satisfy_condtion = false;
						break;
					}
				}

				if (satisfy_condtion) {
					result.add(combinedTuple);
				}

			}
			inner.restart();
		}
		outer.close();
		inner.close();
	}

	private void join(Iterator outer, Iterator inner, Predicate[] conditions) {
		while (outer.hasNext()) {
			Tuple t1 = outer.getNext();
			while (inner.hasNext()) {
				Tuple t2 = inner.getNext();
				Schema combinedSchema = Schema.join(outer.getSchema(),
						inner.getSchema());
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
			inner.restart();
		}
		outer.close();
		inner.close();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		System.out.println("Select * From " + outer.toString());
		outer.explain(depth + 1);
		System.out.print("JOIN " + inner.toString());
		System.out.println("ON");

		for (int i = 0; i < conditions.length; i++)
			System.out.print(conditions[i].toString()
					+ ((i == conditions.length - 1) ? "\n" : " "));
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		// if(isOpen()){
		// index=0;
		// }else {
		// result = new ArrayList<Tuple>();
		// index = 0;
		// join(outer, inner, conditions);
		// }
		outer.restart();
		inner.restart();
		if (outer.hasNext()) {
			current_outer = outer.getNext();
		}
		lastResultComsumed = true;
		open = true;
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return open;
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		open = false;
		// result=null;
		inner.close();
		outer.close();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {

		if (!lastResultComsumed) {
			return true;
		} else {
			if (current_outer == null) {
				return false;
			} else {
				while (inner.hasNext()) {
					Tuple t2 = inner.getNext();
					Schema combinedSchema = Schema.join(outer.getSchema(),
							inner.getSchema());
					Tuple combinedTuple = Tuple.join(current_outer, t2,
							combinedSchema);
					boolean satisfy_condtion = true;

					if (MODE == 1) {
						for (int condition = 0; condition < conditions.length; condition++) {
							if (conditions[condition].evaluate(combinedTuple) == false) {
								satisfy_condtion = false;
								break;
							}
						}
					} else {
						for(int i=0;i<conditions2.length;i++){
							boolean b=false;
							for(int j=0;j<conditions2[i].length;j++){
								if (conditions2[i][j].evaluate(combinedTuple) == true) {
									b=true;
									break;
								}

							}
							if(b==false){
								satisfy_condtion = false;
								break;
							}
						}
						

					}
					if (satisfy_condtion) {
						lastResultComsumed = false;
						current_result = combinedTuple;
						return true;
					}

				}
				while (outer.hasNext()) {
					current_outer = outer.getNext();
					inner.restart();
					while (inner.hasNext()) {
						Tuple t2 = inner.getNext();
						Schema combinedSchema = Schema.join(outer.getSchema(),
								inner.getSchema());
						Tuple combinedTuple = Tuple.join(current_outer, t2,
								combinedSchema);
						boolean satisfy_condtion = true;
						if (MODE == 1) {
							for (int condition = 0; condition < conditions.length; condition++) {
								if (conditions[condition].evaluate(combinedTuple) == false) {
									satisfy_condtion = false;
									break;
								}
							}
						} else {
							for(int i=0;i<conditions2.length;i++){
								boolean b=false;
								for(int j=0;j<conditions2[i].length;j++){
									if (conditions2[i][j].evaluate(combinedTuple) == true) {
										b=true;
										break;
									}

								}
								if(b==false){
									satisfy_condtion = false;
									break;
								}
							}
							

						}
						if (satisfy_condtion) {
							lastResultComsumed = false;
							current_result = combinedTuple;
							return true;
						}

					}

				}

			}

		}
		return false;

	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (hasNext()) {
			lastResultComsumed = true;
			return current_result;
		}
		throw new IllegalStateException("No more tubles");
	}

} // public class SimpleJoin extends Iterator
