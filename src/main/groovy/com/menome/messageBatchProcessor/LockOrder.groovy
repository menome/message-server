package com.menome.messageBatchProcessor

import groovy.transform.CompileStatic

/**
 * A total order on key values, the same in every thread and every batch: what matters is that writers agree, not what
 * the order is. Values of one type compare naturally; values of different types fall back to their class name.
 */
@CompileStatic
class LockOrder {

    static int compare(List<Object> a, List<Object> b) {
        int common = Math.min(a.size(), b.size())
        for (int i = 0; i < common; i++) {
            int byValue = compareValues(a.get(i), b.get(i))
            if (byValue != 0) return byValue
        }
        Integer.compare(a.size(), b.size())
    }

    static int compareValues(Object x, Object y) {
        if (x == y) return 0
        if (x == null) return -1
        if (y == null) return 1
        if (x.getClass() == y.getClass() && x instanceof Comparable) return ((Comparable) x).compareTo(y)
        x.getClass().name.compareTo(y.getClass().name)
    }
}
