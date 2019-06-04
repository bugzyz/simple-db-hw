package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private ArrayList<TDItem> type_fields;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return type_fields.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        int typeAr_size = typeAr.length;
        int fieldAr_size = fieldAr.length;

        assert(typeAr_size >= fieldAr_size);

        type_fields = new ArrayList<TDItem>();

        for (int j = 0, i = 0; i != typeAr_size; j++, i++) {
            if (j < fieldAr_size) {
                type_fields.add(new TDItem(typeAr[i], fieldAr[i]));
            } else {
                type_fields.add(new TDItem(typeAr[i], null));
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        int typeAr_size = typeAr.length;

        assert(typeAr_size > 0);

        type_fields = new ArrayList<TDItem>();
        
        for (int i = 0; i != typeAr_size; i++) {
            TDItem tdi = new TDItem(typeAr[i], null);
            type_fields.add(tdi);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return type_fields.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= type_fields.size())
            throw new NoSuchElementException();
        return type_fields.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= type_fields.size())
            throw new NoSuchElementException();
        return type_fields.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i =0;i!=type_fields.size(); i++) {
            String item_name = type_fields.get(i).fieldName;
            if (item_name != null) {
                if (item_name.equals(name))
                    return i;
            } else {
                if (item_name == name)
                    return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int bytes = 0;
        for (TDItem field : type_fields) {
            bytes += field.fieldType.getLen();
        }
        return bytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        ArrayList<TDItem> al = new ArrayList<TDItem>();
        al.addAll(td1.type_fields);
        al.addAll(td2.type_fields);

        Type[] type_arr = new Type[al.size()];
        String[] name_arr = new String[al.size()];

        for(int i =0; i!=al.size();i++) {
            TDItem tdi = al.get(i);
            type_arr[i] = tdi.fieldType;
            name_arr[i] = tdi.fieldName;
        }

        return new TupleDesc(type_arr, name_arr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == null)
            return false;
        if (!(o instanceof TupleDesc))
            return false;
        if (this == o)
            return true;

        TupleDesc td = (TupleDesc) o;
        if (getSize() != td.getSize())
            return false;

        for (int i = 0; i != type_fields.size(); i++) {
            if (!this.getFieldType(i).equals(td.getFieldType(i)))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String res = new String();

        for (int i = 0; i < type_fields.size(); i++) {
            res += type_fields.get(i).toString();
            if (i != type_fields.size() - 1)
                res += ", ";
        }

        return res;
    }
}
