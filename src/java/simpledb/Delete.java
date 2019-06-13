package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private TupleDesc desc;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // Done
        this.t = t;
        this.child = child;
        desc = new TupleDesc(new Type[]{Type.INT_TYPE});
        called = false;
    }

    public TupleDesc getTupleDesc() {
        // Done
        return desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // Done
        super.open();
        child.open();
        called = false;
    }

    public void close() {
        // Done
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // Done
        child.rewind();
        called = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // Done
        if (called) {
            return null;
        }
        int cnt = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(t, child.next());
                cnt++;
            } catch (IOException e){
                throw new DbException(e.toString());
            }
        }
        Tuple t = new Tuple(desc);
        t.setField(0,new IntField(cnt));
        called = true;
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        // Done
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // Done
        close();
        child = children[0];
    }

}
