package simpledb;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    /*
        map pageid ==>page;
        record the num_pages which can't be changed!
     */
     private final int numPages;
     private  ConcurrentHashMap<PageId,Page>  Map_ID_page;   //lab 1 finished
     private ConcurrentHashMap<PageId,Integer> LRUmap;//lab2 use LRU

    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.Map_ID_page = new ConcurrentHashMap<>();
        this.LRUmap = new ConcurrentHashMap<>();
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
    	BufferPool.pageSize = PAGE_SIZE;
    }
    private void LRUupdate(PageId pid)
    { 
        for (PageId key:LRUmap.keySet())
        {
            int tmp = LRUmap.get(key);
            if(tmp>0)
            {
                LRUmap.put(key, tmp-1);
            }
        }
        //update the accessed
        LRUmap.put(pid, this.numPages);
    }
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        //need to modify  compared with lab1
    
        if(! Map_ID_page.containsKey(pid))
        {   
            //need to evict
            if (Map_ID_page.size() >= this.numPages) 
                evictPage();
            // add it to buffer pool
            DbFile dbfile  = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbfile.readPage(pid);
            Map_ID_page.put(pid,page);
        }
        this.LRUupdate(pid);
        return Map_ID_page.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> affected_page = tableFile.insertTuple(tid, t);
        for (Page page: affected_page)
        {
            PageId pid = page.getId();
            if (!Map_ID_page.containsKey(pid) && Map_ID_page.size() >= this.numPages)
                evictPage();
            page.markDirty(true, tid);
            this.Map_ID_page.put(pid, page);
            LRUupdate(pid);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> affected_page = tableFile.deleteTuple(tid, t);
        for (Page page: affected_page){
            PageId pid = page.getId();
            if (!this.Map_ID_page.containsKey(pid) &&this.Map_ID_page.size() >= this.numPages)
                evictPage();
            page.markDirty(true, tid);
           this.Map_ID_page.put(pid, page);
            LRUupdate(pid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId: this.Map_ID_page.keySet()
        ){
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (!this.Map_ID_page.containsKey(pid))
             return;
        try {
            flushPage(pid);
        } catch (IOException e){
            e.printStackTrace();
        }
        this.Map_ID_page.remove(pid);
        LRUmap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = this.Map_ID_page.get(pid);
        if (page == null) 
            throw new IOException();
        if (page.isDirty() == null) 
            return;
        page.markDirty(false, null);
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        int LRused = this.numPages;
        PageId evict_pageId = null;
        for (PageId pageId : LRUmap.keySet())
        {
            if (LRUmap.get(pageId) <= LRused)
            {
                LRused = LRUmap.get(pageId);
                evict_pageId = pageId;
            }
        }
        if (evict_pageId == null) 
            throw new DbException(" evict noting");
        try
         {
            flushPage(evict_pageId);
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
        this.Map_ID_page.remove(evict_pageId);
        LRUmap.remove(evict_pageId);
    }

}
