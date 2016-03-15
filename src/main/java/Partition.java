import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partition {

    private int idx, begin, end, size, maxLength;

    private Map<Integer, List<Counter>> invertedIndex;

    public Partition(int idx, int begin, int end) {
        this.idx = idx;
        this.begin = begin;
        this.end = end;
        this.size = 0;
        this.maxLength = 0;
        this.invertedIndex = new HashMap<Integer, List<Counter>>();
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public int getBegin() {
        return begin;
    }

    public void setBegin(int begin) {
        this.begin = begin;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    public Map<Integer, List<Counter>> getInvertedIndex() {
        return invertedIndex;
    }

    public void setInvertedIndex(Map<Integer, List<Counter>> invertedIndex) {
        this.invertedIndex = invertedIndex;
    }
}
