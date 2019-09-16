package test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class KevinTool {
    /**
     *
     * @param fileName 文件路径
     *
     * @return
     */
    public static ArrayList<String> readJsonFile(String fileName) {
        String jsonStr = "";
        try {
            File jsonFile = new File(fileName);
            //第一种用字符的方式读
            FileReader fileReader = new FileReader(jsonFile);
            int ch = 0;
            int nw = 0;
            StringBuffer sb = new StringBuffer();
            StringBuffer nextsb = new StringBuffer();
            ArrayList<String> list = new ArrayList<>();
            Character currentWord = null;
            Character nextWord = null;

            while ((ch = fileReader.read()) != -1) {
                //如果现在是}   下一个是} 说明是一个json对象完毕了。
                currentWord = (char) ch;
                if((nw = fileReader.read()) != -1){ //如果能读到一个字符，也就是没到结尾
                    nextWord = (char) nw;
                }
                if((currentWord.equals('}') && nextWord.equals("{"))){
                    //说明是到了当前json对象的结尾
                    sb.append(currentWord);
                    list.add(sb.toString());
                    System.out.println("请空前的stringbuffer"+ sb);
                    sb = new StringBuffer(); //相当于清空当前的stringbuffer
                    System.out.println("请空后的stringbuffer"+ sb);
                    sb.append(nextWord);
                }else if(currentWord.equals('}') && nextWord == null){
                    //说明是到了文件的结尾
                    sb.append(currentWord);
                    list.add(sb.toString());
                   System.out.println("到末尾了");
                }else{
                    System.out.println("啥事儿没有");
                    sb.append(currentWord);
                }
            }

            fileReader.close();
            return list;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


}