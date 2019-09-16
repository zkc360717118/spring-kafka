package  test;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

public class Tool {

    /**
     * 将文件hash取模之后放到不同的小文件中
     *
     * @param targetFile 要去重的文件路径
     * @param splitSize  将目标文件切割成多少份hash取模的小文件个数
     * @return
     */
    public static File[] splitFile(String targetFile, int splitSize) {
        File file = new File(targetFile);//获取去重的文件
        BufferedReader reader = null;//初始化字符输入流
        PrintWriter[] pws = new PrintWriter[splitSize]; //创建字符输出流数组
        File[] littleFiles = new File[splitSize]; // 创建文件数组
        String parentPath = file.getParent();//获取文件所在目录
        File tempFolder = new File(parentPath + File.separator + "test");
        if (!tempFolder.exists()) {
            tempFolder.mkdir();
        }
        for (int i = 0; i < splitSize; i++) {
            littleFiles[i] = new File(tempFolder.getAbsolutePath() + File.separator + i + ".txt");
            if (littleFiles[i].exists()) {
                littleFiles[i].delete();
            }
            try {
                pws[i] = new PrintWriter(littleFiles[i]);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                tempString = tempString.trim();
                if (!tempString.equals("")) {
                    //关键是将每行数据hash取模之后放到对应取模值的文件中，确保hash值相同的字符串都在同一个文件里面
                    int index = Math.abs(tempString.hashCode() % splitSize);
                    pws[index].println(tempString);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            for (int i = 0; i < splitSize; i++) {
                if (pws[i] != null) {
                    pws[i].close();
                }
            }
        }
        return littleFiles;
    }

    /**
     * 对小文件进行去重合并
     *
     * @param littleFiles      切割之后的小文件数组
     * @param distinctFilePath 去重之后的文件路径
     * @param splitSize        小文件大小
     */
    public static void distinct(File[] littleFiles, String distinctFilePath, int splitSize) {
        File distinctedFile = new File(distinctFilePath);
        FileReader[] frs = new FileReader[splitSize];
        BufferedReader[] brs = new BufferedReader[splitSize];
        PrintWriter pw = null;
        try {
            if (distinctedFile.exists()) {
                distinctedFile.delete();
            }
            distinctedFile.createNewFile();
            pw = new PrintWriter(distinctedFile);
            Set<String> unicSet = new HashSet<String>();
            for (int i = 0; i < splitSize; i++) {
                if (littleFiles[i].exists()) {
                    System.out.println("开始对小文件：" + littleFiles[i].getName() + "去重");
                    frs[i] = new FileReader(littleFiles[i]);
                    brs[i] = new BufferedReader(frs[i]);
                    String line = null;
                    while ((line = brs[i].readLine()) != null) {
                        if (line != "") {
                            unicSet.add(line);
                        }
                    }
                    for (String s : unicSet) {
                        pw.println(s);
                    }
                    unicSet.clear();
                    System.gc();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {
            for (int i = 0; i < splitSize; i++) {
                try {
                    if (null != brs[i]) {
                        brs[i].close();
                    }
                    if (null != frs[i]) {
                        frs[i].close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //合并完成之后删除临时小文件
                if (littleFiles[i].exists()) {
                    littleFiles[i].delete();
                }
            }
            if (null != pw) {
                pw.close();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int splitSize = 20;
        File[] files = splitFile("H://tmp//test.txt", splitSize);
        distinct(files, "H://tmp//bigfile-distinct.txt", splitSize);
    }
}