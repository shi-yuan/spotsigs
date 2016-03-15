import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class App {

    public static final Map<String, Integer> keys = new HashMap<String, Integer>(1000000);
    public static HashMap<String, Integer> beadPositions;
    public static int chains;
    public static double confidenceThreshold;
    public static int range;
    public static HashSet<String> stopwords;
    public static String delims = " \t\n\r\f.()\",-:;/\\?!@<>$#&%*+|=_`~'{}[]";
    public static int docId = 0;

    static {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                App.class.getClassLoader().getResource("app.config").openStream()))) {
            String line;
            in.readLine();
            chains = Integer.parseInt(in.readLine());
            beadPositions = new HashMap<String, Integer>(100);
            in.readLine();
            while ((line = in.readLine()) != null) {
                if (line.equals("<ANTECEDENTS>"))
                    continue;
                if (line.equals("</ANTECEDENTS>"))
                    break;
                String[] antecedentInfo = line.split(" ");
                int spotDistance = Integer.parseInt(antecedentInfo[1]);
                beadPositions.put(antecedentInfo[0], spotDistance);
            }

            in.readLine();
            line = in.readLine();
            confidenceThreshold = Double.parseDouble(line);

            in.readLine();
            in.readLine();
            range = Integer.parseInt(in.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }

        stopwords = new HashSet<String>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                App.class.getClassLoader().getResource("stopwords.txt").openStream()))) {
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(tmp.trim(), delims);
                while (tokenizer.hasMoreTokens())
                    stopwords.add(tokenizer.nextToken());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        App app = new App();
        String[] docs = {
                "Driver Job Requirements: Applicant must be at least 21 years old Must live in one of the 48 continental states No more than 2 accidents in the last 3 years No more than 2 moving violations in the last 3 years Any DUI / DWI must be over 5 years old Benefits: Weekly Settlements Fuel Supplement Program All Runs Hub-to-Hub 100% Drop & Hook Outstanding Home Time & MORE!<img src=\"http://img.topusajobs.com/img2/JT12200405/tab.gif\" border=\"0\">",
                "RN-Cath Lab PRN - Weekend Nights-02525-5618 Primary Location: United States-Florida-Jacksonville Job: Nursing - Professional Schedule: Full-time Description The Registered Nurse is responsible for the direct and indirect total nursing care of his/her patient assignment. Responsible for the supervision of assigned personnel. The holding room nurse and chest pain unit nurse are responsible for but not limited to: the care of cardiac/peripheral procedural patients, post sheath removal, recognition of groin complications, EKG interpretation, abnormal lab value recognition and reporting of these values to the physician, input of ACC data. This position requires knowledge of both department and hospital policies and procedures relevant to the delivery of nursing care. Interpretive skills, frequent interactive and consultative associations, along with management and supervisory skills are inherent in the position. The ability to retrieve, communicate or otherwise present information in a written, auditory, or visual fashion is essential. The primary method to express or exchange ideas is through the spoken word. Written, telephone, and manual dexterity skills are required for the position. Qualifications -Graduated from an accredited school of nursing. -Current Florida RN license. BLS & ACLS required. -Minimum of 1 year experience as telemetry RN.<img src=\"http://img.topusajobs.com/img2/HEC1272441/tab.gif\" border=\"0\">"
        };
        String[] docs2 = {
                "Applicant must be at least 21 years old Must live in one of the 48 continental states No more than 2 accidents in the last 3 years No more than 2 moving violations in the last 3 years Any DUI / DWI must be over 5 years old Benefits: Weekly Settlements Fuel Supplement Program All Runs Hub-to-Hub 100% Drop & Hook Outstanding Home Time & MORE!<img src=\"http://img.topusajobs.com/img2/JT12200405/tab.gif\" border=\"0\">"
        };
        List<Counter> counters = new ArrayList<>();
        // 预处理所有文档，提取每个文档的特征集
        for (String doc : docs) {
            counters.add(app.createIndex((++docId) + "", doc, chains, delims, beadPositions, stopwords));
        }

        // 分隔文档向量元素并建立倒排索引
        Partition[] partitions = app.getPartitions(range, confidenceThreshold);
        app.partitioning(partitions, counters);

        // 去重
        List<Counter> counters2 = new ArrayList<>();
        // 预处理所有文档，提取每个文档的特征集
        for (String doc : docs2) {
            counters2.add(app.createIndex((++docId) + "", doc, chains, delims, beadPositions, stopwords));
        }
        for (Counter c : counters2) {
            Set<Counter> duplicates = app.deduplicate(c, partitions, confidenceThreshold);
            if (duplicates != null && !duplicates.isEmpty()) {
                System.out.println("================================");
                for (Counter item : duplicates) {
                    System.out.println(item);
                }
                System.out.println("================================");
            }
        }
    }

    /**
     * 创建索引
     */
    public Counter createIndex(String docid, String content, int chains, String delims, Map<String, Integer> beadPositions, Set<String> stopwords) {
        List<String> words = new ArrayList<String>();
        StringTokenizer tokenizer = new StringTokenizer(content.trim(), delims);
        while (tokenizer.hasMoreTokens())
            words.add(tokenizer.nextToken().toLowerCase());

        String word, token;
        StringBuilder chain;
        Counter counter = new Counter(docid);
        for (Integer i = 0, j, k, pos, length = words.size(); i < length - 1; i++) {
            word = words.get(i);
            if ((pos = beadPositions.get(word)) != null) {
                chain = new StringBuilder();
                k = i + pos;
                for (j = 0; j < chains && k < length; j++) {
                    token = words.get(k);
                    while (stopwords.contains(token) && k < length) {
                        token = words.get(k);
                        k++;
                    }
                    if (!stopwords.contains(token)) {
                        chain.append(token);
                        chain.append(":");
                    }
                    k += pos;
                }
                if (chain.length() > 0) {
                    counter.incrementCount(getKey(chain.toString()));
                }
            }
        }
        System.out.println("READ: " + docid + "\t " + counter.getTotalCount() + " SpotSignatures");
        return counter;
    }

    /**
     * 获取对应的Key
     *
     * @param s spot signature
     * @return
     */
    public Integer getKey(String s) {
        if (keys.get(s) == null) {
            keys.put(s, keys.size() + 1);
        }
        return keys.get(s);
    }

    /**
     * 计算有多少个分隔
     *
     * @param range               最大的文档向量范围
     * @param confidenceThreshold 阈值
     * @return
     */
    public Partition[] getPartitions(int range, double confidenceThreshold) {
        int idx = 0, p = 1, last = p;
        List<Partition> partitions = new ArrayList<Partition>();
        for (; p <= range; p++) {
            if (p - last > (1 - confidenceThreshold) * p) {
                partitions.add(new Partition(idx, last, p + 1));
                last = p + 1;
                idx++;
            }
        }
        partitions.add(new Partition(idx, last, Integer.MAX_VALUE));
        return partitions.toArray(new Partition[partitions.size()]);
    }

    public Partition getPartition(Partition[] partitions, int totalCount) {
        int i = 0, length = partitions.length;
        while (i < length && partitions[i].getEnd() <= totalCount) {
            i++;
        }
        return i == length ? partitions[length - 1] : partitions[i];
    }

    /**
     * 分隔文档向量元素并建立倒排索引
     *
     * @param partitions
     * @param counters
     */
    public void partitioning(Partition[] partitions, List<Counter> counters) {
        Partition partition;
        List<Counter> indexArray;
        // 分隔文档集
        for (Counter counter : counters) {
            partition = getPartition(partitions, counter.getTotalCount());
            for (Integer key : counter.getEntries().keySet()) {
                if ((indexArray = partition.getInvertedIndex().get(key)) == null) {
                    indexArray = new ArrayList<Counter>();
                    partition.getInvertedIndex().put(key, indexArray);
                }
                indexArray.add(counter);
            }
            partition.setSize(partition.getSize() + 1);
            partition.setMaxLength(Math.max(partition.getMaxLength(), counter.getTotalCount()));
        }
        // Sort each index list in descending order of totalLength
        for (Partition p : partitions) {
            for (Integer key : p.getInvertedIndex().keySet()) {
                Collections.sort(p.getInvertedIndex().get(key));
            }
        }
    }

    /**
     * 去重
     *
     * @param counter
     * @return
     */
    public Set<Counter> deduplicate(Counter counter, Partition[] partitions, double confidenceThreshold) {
        Partition partition = getPartition(partitions, counter.getTotalCount());
        // Sort SpotSigs in ascending order of selectivity (DF)
        Map<Integer, Integer> entries = counter.getEntries();
        int i = 0;
        int[] keyArr = new int[entries.size()];
        int[] dfs = new int[entries.size()];
        List<Counter> indexList;
        for (Integer key : entries.keySet()) {
            keyArr[i] = key;
            dfs[i] = (indexList = partition.getInvertedIndex().get(key)) != null ? indexList.size() : Integer.MAX_VALUE;
            i++;
        }
        quickSort(keyArr, dfs, 0, i - 1);

        //
        Set<Counter> duplicates = new HashSet<Counter>(), checked;
        double delta1, delta2;
        int iterations = 1;
        // Check for next partition
        if (partition.getIdx() < partitions.length - 1
                && partition.getEnd() - counter.getTotalCount() <= (1 - confidenceThreshold) * partition.getEnd()) {
            iterations = 2;
        }
        for (int iteration = 0; iteration < iterations; iteration++) {
            delta1 = 0;
            checked = new HashSet<Counter>();
            for (Integer key : keyArr) {
                indexList = partition.getInvertedIndex().get(key);
                for (Counter counter2 : indexList) {
                    delta2 = counter.getTotalCount() - counter2.getTotalCount();
                    if (counter.equals(counter2) || checked.contains(counter2)) {
                        continue;
                    } else if (delta2 < 0 && delta1 - delta2 > (1 - confidenceThreshold) * counter2.getTotalCount()) {
                        continue;
                    } else if (delta2 >= 0 && delta1 + delta2 > (1 - confidenceThreshold) * counter.getTotalCount()) {
                        break;
                    } else if (getJaccard(keyArr, counter, counter2, confidenceThreshold) >= confidenceThreshold) {
                        duplicates.add(counter2);
                        checked.add(counter2);
                    }
                }
                // Early threshold break for inverted index traversal
                delta1 += counter.getCount(key);
                if (delta1 >= (1 - confidenceThreshold) * partition.getMaxLength()) {
                    break;
                }
            }
            // Also check this doc against the next partition
            if (iteration == 0 && iterations == 2) {
                partition = partitions[partition.getIdx() + 1];
            }
        }
        return duplicates;
    }

    /**
     * Jaccard相似度
     */
    public double getJaccard(int[] keys, Counter index1, Counter index2, double threshold) {
        double min, max, s_min = 0, s_max = 0, bound = 0;
        double upper_max = Math.max(index1.getTotalCount(), index2.getTotalCount());
        double upper_union = index1.getTotalCount() + index2.getTotalCount();
        int s_c1 = 0, s_c2 = 0;

        for (int i = 0, c1, c2, len = keys.length; i < len; i++) {
            c1 = index1.getCount(keys[i]);
            c2 = index2.getCount(keys[i]);
            min = Math.min(c1, c2);
            max = Math.max(c1, c2);
            s_min += min;
            s_max += max;
            s_c1 += c1;
            s_c2 += c2;

            // Early threshold break for pairwise counter comparison
            bound += max - min;
            if ((upper_max - bound) / upper_max < threshold)
                return 0;
            else if (s_min / upper_union >= threshold)
                return 1;
        }

        return s_min / (s_max + (index1.getTotalCount() - s_c1) + (index2.getTotalCount() - s_c2));
    }

    /**
     * 快速排序
     */
    private int partition(int[] keys, int[] counts, int low, int high) {
        int i = low - 1, j = high + 1, pivotK = counts[(low + high) / 2], temp;
        while (i < j) {
            i++;
            while (counts[i] < pivotK) {
                i++;
            }
            j--;
            while (counts[j] > pivotK) {
                j--;
            }
            if (i < j) {
                temp = keys[i];
                keys[i] = keys[j];
                keys[j] = temp;
                temp = counts[i];
                counts[i] = counts[j];
                counts[j] = temp;
            }
        }
        return j;
    }

    private void quickSort(int[] keys, int[] counts, int low, int high) {
        if (low >= high)
            return;
        int p = partition(keys, counts, low, high);
        quickSort(keys, counts, low, p);
        quickSort(keys, counts, p + 1, high);
    }
}
