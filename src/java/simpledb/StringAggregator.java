package simpledb;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;

import simpledb.IntField;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield, afield;
    private final Op op;
    Type gbfieldtype;
    private final Map<Field, Integer> groupResult;
    private TupleDesc td;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
        this.op = what;

        this.td = (gbfieldtype == null) 
                ? new TupleDesc(new Type[] { Type.INT_TYPE }) : 
                  new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });

        groupResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = gbfield == NO_GROUPING ? new StringField("", 0) {
            @Override
            public int hashCode() {
                return 0;
            }
        } : tup.getField(gbfield);

        if(groupResult.get(key) == null) {
            groupResult.put(key, 0);
        }
        
        switch (op) {
            case COUNT:
            groupResult.put(key, groupResult.get(key) + 1);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> resultList = new ArrayList<>();

        for (Map.Entry<Field, Integer> entry : groupResult.entrySet()) {
            Tuple temp = new Tuple(this.td);

            if (td.numFields() != 1)
                temp.setField(0, entry.getKey());

            temp.setField(td.numFields() != 1 ? 1 : 0, new IntField(entry.getValue()));

            resultList.add(temp);
        }
        
        // some code goes here
        return new OpIterator() {
            private static final long serialVersionUID = 1L;
            private Iterator<Tuple> iter;

            public void open() throws DbException, TransactionAbortedException {
                iter = resultList.iterator();
            }
        
            public TupleDesc getTupleDesc() {
                return td;
            }
        
            public boolean hasNext() throws TransactionAbortedException, DbException {
                return iter.hasNext();
            }
        
            public Tuple next() throws NoSuchElementException,
                    TransactionAbortedException, DbException {
                return iter.next();
            }
        
            public void close() {
                iter = null;
            }
        
            public void rewind() throws DbException, NoSuchElementException,
                    TransactionAbortedException {
                close();
                open();
            }
        };
    }

}
