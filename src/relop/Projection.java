package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
	private Iterator iterator;
	private Integer[] fields;
	private Schema newSchema;

	/**
	 * Constructs a projection, given the underlying iterator and field numbers.
	 */
	public Projection(Iterator iter, Integer... fields) {
		this.iterator = iter;
		this.fields = fields.clone();
		this.setSchema(iter.getSchema());
		newSchema = new Schema(fields.length);
		Schema oldSchema = iter.getSchema();
		for (int i = 0; i < fields.length; i++) {
			int oldPlace = fields[i];
			newSchema.initField(i, oldSchema.fieldType(oldPlace),
					oldSchema.fieldLength(oldPlace),
					oldSchema.fieldName(oldPlace));
		}
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		System.out.print("Select ");
		for (int i = 0; i < fields.length; i++)
			System.out.print(fields[i]
					+ ((i == fields.length - 1) ? "\n" : " "));
		System.out.println("From " + iterator.toString());
		iterator.explain(depth + 1);
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		iterator.restart();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return iterator.isOpen();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		iterator.close();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return iterator.hasNext();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (isOpen() && hasNext()) {
			Tuple completeTuple = iterator.getNext();
			Object[] values = new Object [fields.length];
			for(int i = 0; i< values.length;i++)
				values[i] = completeTuple.getField(fields[i]);
			Tuple projectedTuple = new Tuple(newSchema, values);
			return projectedTuple;
		}
		throw new IllegalStateException("No More Tuples.");
	}

} // public class Projection extends Iterator
