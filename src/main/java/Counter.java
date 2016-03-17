import java.util.HashMap;
import java.util.Map;

/**
 * This is a simple hash aggregator for counting signatures
 */
public class Counter implements Comparable<Counter> {

    private String docId;
    private int totalCount;
    private Map<Integer, Integer> signatures;

    public Counter(String docId) {
        this.docId = docId;
        this.totalCount = 0;
        this.signatures = new HashMap<Integer, Integer>();
    }

    public int getCount(int key) {
        return signatures.containsKey(key) ? signatures.get(key) : 0;
    }

    public void incrementCount(int key) {
        signatures.put(key, getCount(key) + 1);
        totalCount++;
    }

    /**
     * Sort by signature length
     */
    @Override
    public int compareTo(Counter c) {
        return Integer.compare(c.totalCount, this.totalCount);
    }

    @Override
    public int hashCode() {
        return this.docId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return docId.equals(((Counter) o).docId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(docId + "=[");
        int size = signatures.size();
        Integer[] keys = signatures.keySet().toArray(new Integer[size]);
        for (int i = 0; i < size; ++i) {
            sb.append(keys[i]).append(":").append(getCount(keys[i])).append(i < size - 1 ? ", " : "");
        }
        sb.append("] @ ").append(totalCount);
        return sb.toString();
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

    public Map<Integer, Integer> getSignatures() {
        return signatures;
    }

    public void setSignatures(Map<Integer, Integer> signatures) {
        this.signatures = signatures;
    }
}
