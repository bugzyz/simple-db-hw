package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    private LockManager lockManager;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private ConcurrentHashMap<PageId, Page> pages;
    private final int numPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // Done
        pages = new ConcurrentHashMap<PageId, Page>();
        this.numPages = numPages;
        lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    private enum LockType {
        EXCLUSIVE, SHARED
    }

    private class LockManager {
        private class Lock {
            LockType lockType;
            Vector<TransactionId> holdLockTxs;

            public Lock(LockType lockType) {
                this.holdLockTxs = new Vector<>();
                this.lockType = lockType;
            }

            public void setType(LockType lockType) {
                this.lockType = lockType;
            }

            public LockType getType() {
                return lockType;
            }

            public boolean isEmpty() {
                return holdLockTxs.size() == 0;
            }

            public int size() {
                return holdLockTxs.size();
            }

            public boolean containsTx(TransactionId tid) {
                return holdLockTxs.contains(tid);
            }

            public boolean addTx(TransactionId tid) {
                if (!containsTx(tid)) {
                    holdLockTxs.add(tid);
                    return true;
                }
                return false;
            }

            public boolean removeTx(TransactionId tid) {
                if (containsTx(tid)) {
                    holdLockTxs.remove(tid);
                    return true;
                }
                return false;
            }
        }

        private ConcurrentHashMap<PageId, Lock> pid2lock;

        public LockManager() {
            pid2lock = new ConcurrentHashMap<>();
        }

        public synchronized boolean acquireLock(PageId pid, TransactionId tid, LockType lockType) {
            // There's no lock on the target page, create one and return true
            if (pid2lock.get(pid) == null) {
                Lock newLock = new Lock(lockType);
                newLock.addTx(tid);
                pid2lock.put(pid, newLock);
                
                return true;
            }
            
            // Already have a lock
            Lock targetLock = pid2lock.get(pid);

            // tid own the targetLock
            if (targetLock.containsTx(tid)) {
                // tid is exclusive targetLock's only owner
                if (targetLock.getType() == LockType.EXCLUSIVE) {
                    // requiring shared or exclusive lock are both ok
                    assert targetLock.size() == 1 : "targetlock have more than one tid in exclusive mode";
                    return true;
                }

                // tid is shared targetLock's only owner
                if (targetLock.size() == 1) {
                    // upgrade to exclusive lock no matter acquiring lockType is shared or exclusive
                    targetLock.setType(LockType.EXCLUSIVE);
                    return true;
                }

                // tid is one of the owner of shared targetLock
                // Acquiring a exclusive lock is forbiddened
                if (lockType == LockType.EXCLUSIVE)
                    return false;

                // Acquiring a shared lock is allowed
                return true;
            }

            // tid doesn't own the exclusive targetLock
            if (targetLock.getType() == LockType.EXCLUSIVE)
                return false;

            // tid doesn't own the shared targetLock
            if (lockType == LockType.SHARED) {
                // allow shared lock join
                targetLock.addTx(tid);
                return true;
            }

            // forbidden exclusive lock join
            return false;
        }

        public synchronized boolean releaseLock(PageId pid, TransactionId tid) {
            Lock targetLock = pid2lock.get(pid);

            // There isn't a lock for pid
            if (targetLock == null)
                return false;

            // Remove tid success
            if (targetLock.removeTx(tid)) {
                // If targetLock no more owned by any tid, remove it
                if (targetLock.isEmpty())
                    pid2lock.remove(pid);

                return true;
            }

            // Remove tid failed that tid doesn't own targetLock
            return false;
        }

        public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
            Lock targetLock = pid2lock.get(pid);

            // if not a single lock is held on pid
            if (targetLock == null)
                return false;

            return targetLock.containsTx(tid);
        }

        public synchronized void releaseTxLocks(TransactionId tid) {
            for (PageId pid : pid2lock.keySet()) {
                if (holdsLock(pid, tid))
                    releaseLock(pid, tid);
            }
        }
    }
    /**
     * Retrieve the specified page with the associated permissions. Will acquire a
     * lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is present,
     * it should be returned. If it is not present, it should be added to the buffer
     * pool and returned. If there is insufficient space in the buffer pool, a page
     * should be evicted and the new page should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // Done
        long start = System.currentTimeMillis();
        long timeOut = new Random().nextInt(2000) + 1000;
        while (!lockManager.acquireLock(pid, tid,
                perm == Permissions.READ_ONLY ? LockType.SHARED : LockType.EXCLUSIVE)) {
            // Looping until acquire the requring lock

            // Dead lock detection
            try {
                Thread.sleep(50);
            } catch (Exception e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if (now - start > timeOut)
                throw new TransactionAbortedException();
        }

        if (pages.get(pid) != null)
            return pages.get(pid);

        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        HeapPage hp = (HeapPage) hf.readPage(pid);

        // If the page number surpass the limit, we evict and put
        if (pages.size() < numPages) {
            pages.put(pid, hp);
        } else {
            evictPage();
            pages.put(pid, hp);
        }

        return pages.get(pid);
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in
     * wrong behavior. Think hard about who needs to call this and why, and why they
     * can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // Done
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // Done
        lockManager.releaseTxLocks(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // Done
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        // Done
        if (commit) {
            flushPages(tid);
        } else {
            restorePages(tid);
        }

        transactionComplete(tid);
    }

    private synchronized void restorePages(TransactionId tid) {
        // Restore the page from disk to cover the dirty pages
        for (PageId pid : pages.keySet()) {
            Page page = pages.get(pid);

            if (page.isDirty() == tid) {
                int tabId = pid.getTableId();
                DbFile file = Database.getCatalog().getDatabaseFile(tabId);
                Page pageFromDisk = file.readPage(pid);

                pages.put(pid, pageFromDisk);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will acquire
     * a write lock on the page the tuple is added to and any other pages that are
     * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
     * cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // Done
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);

        ArrayList<Page> pageList = file.insertTuple(tid, t);

        // after inserted, tuple will get a record Id, then we can mark page dirty
        for (Page page : pageList) {
            page.markDirty(true, tid);
            pages.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on
     * the page the tuple is removed from and any other pages that are updated. May
     * block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // Done
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        HeapPage page = (HeapPage) getPage(tid, pid, Permissions.READ_WRITE);

        page.deleteTuple(t);
        page.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
     * dirty data to disk so will break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // Done
        for (ConcurrentHashMap.Entry<PageId, Page> entry : pages.entrySet()) {
            if (entry.getValue().isDirty() != null) {
                flushPage(entry.getKey());
            }
        }

    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in its
     * cache.
     * 
     * Also used by B+ tree files to ensure that deleted pages are removed from the
     * cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // Done
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // Done
        Page page = pages.get(pid);
        TransactionId tid = page.isDirty();
        if (tid != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, tid);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // Done
        for (Page page : pages.values()) {
            if (page == null)
                continue;

            TransactionId checkingTid = page.isDirty();

            // If the page isn't dirty then continue
            if (checkingTid == null)
                continue;

            // If the page is dirty and equal to tid then flush
            if (checkingTid.equals(tid))
                flushPage(page.getId());
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // Done
        // Pick a dirty page to evict
        for (ConcurrentHashMap.Entry<PageId, Page> entry : pages.entrySet()) {
            if (entry.getValue().isDirty() == null) {
                discardPage(entry.getKey());
                return;
            }
        }

        throw new DbException("no page can be used to or should to be evicted");
    }

}
