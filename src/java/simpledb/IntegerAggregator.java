package simpledb;

import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

import simpledb.IntField;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield, afield;
    private final Op op;
    Type gbfieldtype;
    private final Map<Field, GroupEntity> groupResult;
    private TupleDesc td;
    
    private class GroupEntity {
        private Op aggregateOp;
        private Field f;
        private Integer aggregateResult;
        private Integer count = 0;

        public GroupEntity(Field f, Op aggregateOp) {
            this.f = f;
            this.aggregateOp = aggregateOp;

            switch(aggregateOp) {
                case MIN:
                this.aggregateResult = Integer.MAX_VALUE;
                break;
                case MAX:
                this.aggregateResult = Integer.MIN_VALUE;
                break;
                case SUM:
                case AVG:
                case COUNT:
                this.aggregateResult = 0;
                break;
                default:
                throw new UnsupportedOperationException();
            }
        }

        public void mergeIntField(IntField field) throws UnsupportedOperationException {
            switch(aggregateOp) {
                case MIN:
                aggregateResult = Math.min(field.getValue(), aggregateResult);
                break;
                case MAX:
                aggregateResult = Math.max(field.getValue(), aggregateResult);
                break;
                case SUM:
                case AVG:
                aggregateResult += field.getValue();
                break;
                case COUNT:
                break;
                default:
                throw new UnsupportedOperationException();
            }
            count++;
        }

        public Tuple getResult(TupleDesc td) throws UnsupportedOperationException {
            Tuple retTuple = new Tuple(td);
            if (td.numFields() != 1)
                retTuple.setField(0, this.f);
            switch (aggregateOp) {
            case MIN:
            case MAX:
            case SUM:
                retTuple.setField(td.numFields() != 1 ? 1 : 0, new IntField(aggregateResult));
                break;
            case AVG:
                retTuple.setField(td.numFields() != 1 ? 1 : 0, new IntField(aggregateResult / count));
                break;
            case COUNT:
                retTuple.setField(td.numFields() != 1 ? 1 : 0, new IntField(count));
                break;
            default:
                throw new UnsupportedOperationException();
            }
            
            return retTuple;
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = gbfield == NO_GROUPING ? new IntField(0) {
            @Override
            public int hashCode() {
                return 0;
            }
        } : tup.getField(gbfield);

        if(groupResult.get(key) == null) {
            groupResult.put(key, new GroupEntity(key, op));
        }

        groupResult.get(key).mergeIntField((IntField)tup.getField(afield));
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> resultList = new ArrayList<>();
        
        for(Map.Entry<Field, GroupEntity> entry : groupResult.entrySet()) {
            resultList.add(entry.getValue().getResult(td));
        }

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
