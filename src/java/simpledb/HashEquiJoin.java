package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate pred;
    private DbIterator child1, child2;
    private Tuple left, right;
    private Map<Field, ArrayList<Tuple>> map = new ConcurrentHashMap<>();

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.pred = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
        return child1.getTupleDesc().getFieldName(pred.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return child2.getTupleDesc().getFieldName(pred.getField2());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        map.clear();
        while (child2.hasNext()){
            right = child2.next();
            Field key = right.getField(pred.getField2());
            if (!map.containsKey(key)) map.put(key, new ArrayList<>());
            ArrayList<Tuple> Tuplelist = map.get(key);
            Tuplelist.add(right);
        }
        super.open();
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        map.clear();
        listIt = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    transient Iterator<Tuple> listIt = null;
    private Tuple mergeTuple(){
        right = listIt.next();

        int num1 = left.getTupleDesc().numFields();
        int num2 = right.getTupleDesc().numFields();

        Tuple next = new Tuple(this.getTupleDesc());
        for (int i = 0; i < num1; i++)
            next.setField(i, left.getField(i));
        for (int i = 0; i < num2; i++)
            next.setField(num1 + i, right.getField(i));
        return next;
    }
    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (listIt != null && listIt.hasNext()){
            return mergeTuple();
        }

        while (child1.hasNext()){
            left = child1.next();
            Field key = left.getField(pred.getField1());

            ArrayList<Tuple> matchTupleList = map.get(key);
            if (matchTupleList == null) continue;
            listIt = matchTupleList.iterator();
            return fetchNext();
        }

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
         child1 = children[0];
        child2 = children[1];
    }
    
}
