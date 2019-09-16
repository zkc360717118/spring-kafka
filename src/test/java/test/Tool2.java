package test;

import java.io.*;
import java.util.HashSet;

public class Tool2 {
    public static File[] splitFile(String path,int split,int k) {
        //获取被分割的文件
        File file = new File(path);
        BufferedReader bufferedReader = null;//初始化字符输入流
        // 初始化要分割的文件
        File[] splitsFileArr = new File[split];
        PrintWriter[] splitsFileWriters = new PrintWriter[split];

        //初始化分割文件所在的目录test
        File testTempFile = new File(file.getParent()+file.separator+"test"+k);
        if(!testTempFile.exists()){
            testTempFile.mkdir();
        }


        try {
            //生成分割文件 和 对应的输出流
            for (int i = 0; i < split; i++) {
                splitsFileArr[i] = new File(testTempFile.getAbsolutePath()+testTempFile.separator+i+".txt");
                if(!splitsFileArr[i].exists()){
                    splitsFileArr[i].createNewFile();
                }

                splitsFileWriters[i] = new PrintWriter(splitsFileArr[i]);//创建输入流
            }
            // 获取被分割的文件
            bufferedReader = new BufferedReader(new FileReader(file));//用Buffereader方便一行行读取

            // 读取被分割文件到各个分割文件
            String tmpString = null;
            while( (tmpString = bufferedReader.readLine()) != null){
//                System.out.println(tmpString);
                tmpString = tmpString.trim();
                if(!tmpString.equals("")){
                    //取模 -》取整
                    int splitFilePos = Math.abs((tmpString.hashCode()) % split);
                    //-》 放入到各个文件 从而保证重复的数据在 同一个文件
                    splitsFileWriters[splitFilePos].println(tmpString);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            // 关闭文件字符输入流
            if(bufferedReader !=null){
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            //关闭字符输出流
            for(int i=0; i<split; i++){
                splitsFileWriters[i].close();
            }
        }

        return splitsFileArr;
    }

    public static void distinct(File[] files,String path,int size) {
        //初始化输出文件
        File file = new File(path);
        //初始化字符输入流
        FileReader[] fileReaders = new FileReader[size];
        BufferedReader[] bufferedReaders = new BufferedReader[size];
        //初始化字符输入流
        PrintWriter pw=null;


        try {
            //先创建目标文件
            if(file.exists()){
                file.delete();
            }
            file.createNewFile();

            //创建初始化字符输出流
            pw = new PrintWriter(file);

            HashSet<String> set = new HashSet<>();

            //循环遍历每个分割后的文件
            for (int i = 0; i < size; i++) {
                fileReaders[i] = new FileReader(files[i]);
                bufferedReaders[i] = new BufferedReader(fileReaders[i]);
                String line = null;
                while( (line = bufferedReaders[i].readLine()) !=null){
                    System.out.println(line);
                    set.add(line);
                }
                //单个文件去重完毕写入目标文件
                for(String s : set){
                    pw.println(s);
                }
                //手动触发一次gc
                System.gc();
                //重置hashset
                set.clear();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(pw != null){
                pw.close();
            }

            for (int i = 0; i < size; i++) {
                try {
                    bufferedReaders[i].close();
                    fileReaders[i].close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        }
    }

    public static void distinct2(File[] files,File[] files2,String path,String path2,int size) {
        //初始化输出文件1 2
        File file = new File(path);
        File file2 = new File(path2);
        //初始化字符输入流 针对文件1
        FileReader[] fileReaders = new FileReader[size];
        BufferedReader[] bufferedReaders = new BufferedReader[size];

        //初始化字符输入流 针对文件2
        FileReader[] fileReaders2 = new FileReader[size];
        BufferedReader[] bufferedReaders2 = new BufferedReader[size];

        //初始化字符输出流
        PrintWriter pw=null;
        PrintWriter pw2=null;


        try {
            //先创建目标文件
            if(file.exists()){
                file.delete();
            }
            if(file2.exists()){
                file2.delete();
            }
            file.createNewFile();
            file2.createNewFile();

            //创建初始化字符输入流
            pw = new PrintWriter(file);
            pw2 = new PrintWriter(file2);

            HashSet<String> set = new HashSet<>();
            HashSet<String> set2 = new HashSet<>();
            HashSet<String> unionset = new HashSet<>();


            //循环遍历每个分割后的文件
            for (int i = 0; i < size; i++) {
                fileReaders[i] = new FileReader(files[i]);
                bufferedReaders[i] = new BufferedReader(fileReaders[i]);
                String line = null;
                while( (line = bufferedReaders[i].readLine()) !=null){
//                    System.out.println("文件一"+line);
                    set.add(line);
                }

                fileReaders2[i] = new FileReader(files2[i]);
                bufferedReaders2[i] = new BufferedReader(fileReaders2[i]);
                String line2 = null;
                while( (line2 = bufferedReaders2[i].readLine()) !=null){
//                    System.out.println("文件2"+line);
                    set2.add(line2);
                }

                //判断
                unionset.addAll(set);
                unionset.removeAll(set2); //获取第一个文件的数据（不含第二个文件）
                //第一个文件去重完毕写入目标文件
                for(String s : unionset){
                    pw.println(s);
                }
                unionset.clear();
                unionset.addAll(set2);
                unionset.removeAll(set);
                for(String s : unionset){
                    pw2.println(s);
                }
                //手动触发一次gc
                System.gc();
                //重置hashset
                set.clear();
                set2.clear();
                unionset.clear();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(pw != null){
                pw.close();
                pw2.close();
            }

            for (int i = 0; i < size; i++) {
                try {
                    bufferedReaders[i].close();
                    fileReaders[i].close();
                    bufferedReaders2[i].close();
                    fileReaders2[i].close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        }
    }

    public static void main(String[] args) {
        int splitSize = 10;

        File[] files1 = splitFile("H://tmp//test.txt", splitSize,1);
        for (int i=0; i<files1.length;i++){
            System.out.println(files1[i].getName());

        }

        File[] files2 = splitFile("H://tmp//test2.txt", splitSize,2);
        for (int i=0; i<files2.length;i++){
            System.out.println(files2[i].getName());

        }
//        distinct2(files1,files1, "H://tmp//bigfile-distinct.txt", splitSize);
        distinct2(files1,files2, "H://tmp//test1-distinct.txt","H://tmp//test2-distinct.txt", splitSize);
    }
}
