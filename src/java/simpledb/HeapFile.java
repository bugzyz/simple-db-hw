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
        // Done
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // Done
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
        // Done
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // Done
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // Done
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
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // Done
        PageId pid = page.getId();
        int pgNo = pid.getPageNumber();
        int pgSize = BufferPool.getPageSize();

        try {
            RandomAccessFile randAccFile = new RandomAccessFile(f, "rw");
            randAccFile.seek(pgNo * pgSize);
            randAccFile.write(page.getPageData());
            randAccFile.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // Done
        return (int)(this.f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // Done
        HeapPageId pid = new HeapPageId(getId(), 0);
        // Find a available heap page id
        while (pid.getPageNumber() < numPages()) {
            HeapPage checkPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (checkPage.getNumEmptySlots() != 0)
                break;
            else
                pid = new HeapPageId(getId(), pid.getPageNumber() + 1);
        }

        assert(pid.getPageNumber() <= numPages());
        
        // Find no available page
        // Create a new heap page and write it to bufferpool
        if (pid.getPageNumber() == numPages()) {
            byte[] data = HeapPage.createEmptyPageData();
            HeapPage newPage = new HeapPage(pid, data);
            writePage(newPage);
        }

        HeapPage targetPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

        targetPage.insertTuple(t);

        ArrayList<Page> retList = new ArrayList<Page>();
        retList.add(targetPage);

        return retList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // Done
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        // delete tuple and mark page as dirty
        HeapPage targetPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        targetPage.deleteTuple(t);

        ArrayList<Page> retList = new ArrayList<Page>();
        retList.add(targetPage);

        return retList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // Done
        return new DbFileIterator() {
            private int curPageId = 0;
            private Iterator<Tuple> tupleIt = null;

            private Iterator<Tuple> getTupleIteratorOfPageId(int pageId)
                    throws DbException, TransactionAbortedException {
                HeapPageId hpid = new HeapPageId(getId(), pageId);

                HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);

                return hp.iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                curPageId = 0;
                tupleIt = getTupleIteratorOfPageId(curPageId++);
            }
    
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(tupleIt == null) return false;
                if(tupleIt.hasNext()) return true;

                int checkPageId = curPageId;
                while (checkPageId < numPages() && !getTupleIteratorOfPageId(checkPageId).hasNext()) {
                    checkPageId++;
                }
                return checkPageId < numPages();
            }
    
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(tupleIt == null) throw new NoSuchElementException();
                if(tupleIt.hasNext()) return tupleIt.next();

                while (curPageId < numPages() && !getTupleIteratorOfPageId(curPageId).hasNext()) {
                    curPageId++;
                }

                if(curPageId < numPages()) {
                    tupleIt = getTupleIteratorOfPageId(curPageId++);
                    return tupleIt.next();
                }
                throw new NoSuchElementException();
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

