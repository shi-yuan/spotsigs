import java.util.HashMap;
import java.util.Map;

/**
 * This is a simple hash aggregator for counting signatures
 */
public class Counter implements Comparable<Counter> {

    private String docId;
    private int totalCount;
    private Map<Integer, Integer> entries;

    public Counter(String docId) {
        this.docId = docId;
        this.totalCount = 0;
        this.entries = new HashMap<Integer, Integer>();
    }

    public int getCount(int key) {
        return entries.containsKey(key) ? entries.get(key) : 0;
    }

    public void incrementCount(int key) {
        entries.put(key, getCount(key) + 1);
        totalCount++;
    }

    /**
     * Sort by signature length
     */
    @Override
    public int compareTo(Counter c) {
        return Integer.compare(c != null ? c.totalCount : 0, totalCount);
    }

    @Override
    public int hashCode() {
        return this.docId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Counter counter = (Counter) o;
        return docId.equals(counter.docId);
    }

    public String toString() {
        String s = docId + "=[";
        Integer[] keys = entries.keySet().toArray(new Integer[entries.size()]);
        for (int i = 0; i < entries.size(); i++) {
            s += String.valueOf(keys[i]) + ":" + String.valueOf(getCount(keys[i]))
                    + (i < entries.size() - 1 ? ", " : "");
        }
        s += "] @ " + String.valueOf(totalCount);
        return s;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public Map<Integer, Integer> getEntries() {
        return entries;
    }

    public void setEntries(Map<Integer, Integer> entries) {
        this.entries = entries;
    }
}
