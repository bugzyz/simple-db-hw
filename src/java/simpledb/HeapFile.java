package simpledb;

import java.io.*;
import java.security.Permission;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private TupleDesc td;
    private File f;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        byte[] data = new byte[pageSize];

        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            raf.seek(pgNo * pageSize);
            raf.read(data);
            raf.close();
            return new HeapPage((HeapPageId)pid, data);
        } catch (Exception e) {
            System.out.println("file not found, me print");
            return null;
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(this.f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int curPageId = 0;
            private Iterator<Tuple> tupleIt;

            private Iterator<Tuple> getTupleIteratorOfPageId(int pageId)
                    throws DbException, TransactionAbortedException {
                HeapPageId hpid = new HeapPageId(getId(), pageId);

                HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);

                return hp.iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                tupleIt = getTupleIteratorOfPageId(curPageId++);
            }
    
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(tupleIt ==null) return false;
                if(tupleIt.hasNext()) return true;
                if(curPageId >= numPages()) return false;

                tupleIt = getTupleIteratorOfPageId(curPageId++);
                return hasNext();
            }
    
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!hasNext()) throw new NoSuchElementException();
                if(tupleIt.hasNext()) return tupleIt.next();

                tupleIt = getTupleIteratorOfPageId(curPageId++);
                return tupleIt.next();
            }
    
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }
    
            @Override
            public void close() {
                tupleIt = null;
                curPageId = 0;
            }
        };
    }

}

