import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partition {

    private int index, begin, end, size;

    private Map<Integer, List<Counter>> invertedIndex;

    public Partition(int index, int begin, int end) {
        this.index = index;
        this.begin = begin;
        this.end = end;
        this.size = 0;
        this.invertedIndex = new HashMap<Integer, List<Counter>>();
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
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

    public Map<Integer, List<Counter>> getInvertedIndex() {
        return invertedIndex;
    }

    public void setInvertedIndex(Map<Integer, List<Counter>> invertedIndex) {
        this.invertedIndex = invertedIndex;
    }
}
