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
            Counter c = app.createIndex((++docId) + "", doc, chains, delims, beadPositions, stopwords);
            if (0 < c.getTotalCount()) {
                counters.add(c);
            }
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
            if (0 < c.getTotalCount()) {
                Set<Counter> duplicates = app.deduplicate(c, partitions, confidenceThreshold);
                if (duplicates != null && !duplicates.isEmpty()) {
                    System.out.println("================================");
                    for (Counter item : duplicates) {
                        System.out.println(item);
                    }
                    System.out.println("================================");
                }
            } else {
                // TODO: 如果没有特征
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

        String word;
        StringBuilder chain;
        Counter counter = new Counter(docid);
        for (Integer i = 0, pos, length = words.size(); i < length - 1; ++i) {
            word = words.get(i);
            if ((pos = beadPositions.get(word)) != null) {
                chain = new StringBuilder();
                for (int j = i + pos, c = chains; j < length; ) {
                    if (!stopwords.contains(words.get(j))) {
                        chain.append(":").append(words.get(j));
                        if (--c <= 0) {
                            break;
                        }
                        j += pos;
                    } else {
                        ++j;
                    }
                }
                if (chain.length() > 0) {
                    System.out.println(word + chain.toString());
                    counter.incrementCount(getKey(word + chain.toString()));
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
        int index = 0, p = 1, last = p;
        List<Partition> partitions = new ArrayList<Partition>();
        for (; p <= range; p++) {
            if (p - last > (1 - confidenceThreshold) * p) {
                partitions.add(new Partition(index, last, p + 1));
                last = p + 1;
                index++;
            }
        }
        partitions.add(new Partition(index, last, Integer.MAX_VALUE));
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
            for (Integer key : counter.getSignatures().keySet()) {
                if ((indexArray = partition.getInvertedIndex().get(key)) == null) {
                    indexArray = new ArrayList<Counter>();
                    partition.getInvertedIndex().put(key, indexArray);
                }
                indexArray.add(counter);
            }
            partition.setSize(partition.getSize() + 1);
        }
        // 按文档向量的长度从大到小进行排序
        for (Partition p : partitions) {
            for (Integer key : p.getInvertedIndex().keySet()) {
                Collections.sort(p.getInvertedIndex().get(key));
            }
        }
    }

    /**
     * 去重
     */
    public Set<Counter> deduplicate(Counter counter, Partition[] partitions, double confidenceThreshold) {
        Partition partition = getPartition(partitions, counter.getTotalCount());
        // 按SpotSigs在文档中的出现频率升序排序
        List<Integer> keyList = new ArrayList<Integer>(counter.getSignatures().keySet());
        final Map<Integer, List<Counter>> invertedIndex = partition.getInvertedIndex();
        Collections.sort(keyList, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                List<Counter> a = invertedIndex.get(o1), b = invertedIndex.get(o2);
                return (a != null ? a.size() : Integer.MAX_VALUE) - (b != null ? b.size() : Integer.MAX_VALUE);
            }
        });

        // Check for next partition
        int iterations = 1;
        if (partition.getIndex() < partitions.length - 1
                && partition.getEnd() - counter.getTotalCount() <= (1 - confidenceThreshold) * partition.getEnd()) {
            iterations = 2;
        }
        Set<Counter> duplicates = new HashSet<Counter>(), checked, set;
        double delta1, delta2;
        List<Counter> indexList;
        for (int iteration = 0, maxLength; iteration < iterations; iteration++) {
            delta1 = 0;
            checked = new HashSet<Counter>();
            for (Integer key : keyList) {
                indexList = partition.getInvertedIndex().get(key);
                if (indexList != null) {
                    for (Counter counter2 : indexList) {
                        delta2 = counter.getTotalCount() - counter2.getTotalCount();
                        if (counter.equals(counter2) || checked.contains(counter2)) {
                            continue;
                        } else if (delta2 < 0 && delta1 - delta2 > (1 - confidenceThreshold) * counter2.getTotalCount()) {
                            continue;
                        } else if (delta2 >= 0 && delta1 + delta2 > (1 - confidenceThreshold) * counter.getTotalCount()) {
                            break;
                        } else if (getJaccard(counter, counter2) >= confidenceThreshold) {
                            duplicates.add(counter2);
                            checked.add(counter2);
                        }
                    }
                }

                // Early threshold break for inverted index traversal
                set = new HashSet<Counter>();
                for (List<Counter> list : partition.getInvertedIndex().values()) {
                    set.addAll(list);
                }
                set.removeAll(checked);
                maxLength = 0;
                for (Counter item : set) {
                    maxLength = Math.max(maxLength, item.getTotalCount());
                }
                delta1 += counter.getCount(key);
                if (delta1 >= (1 - confidenceThreshold) * maxLength) {
                    break;
                }
            }
            // Also check this doc against the next partition
            if (iteration == 0 && iterations == 2) {
                partition = partitions[partition.getIndex() + 1];
            }
        }
        return duplicates;
    }

    /**
     * Jaccard相似度
     */
    public double getJaccard(Counter index1, Counter index2) {
        Set<Integer> keySet = new HashSet<Integer>(index1.getSignatures().keySet());
        keySet.addAll(index2.getSignatures().keySet());
        double s_min = 0.0, s_max = 0.0;
        int c1, c2;
        for (Integer key : keySet) {
            c1 = index1.getCount(key);
            c2 = index2.getCount(key);
            if (c1 <= c2) {
                s_min += c1;
                s_max += c2;
            } else {
                s_min += c2;
                s_max += c1;
            }
        }
        return s_min / s_max;
    }
}
