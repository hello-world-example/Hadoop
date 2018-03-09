package xyz.kail.demo.hbase.core;

/**
 * TODO 注释
 *
 * @author kaibin.yang@ttpai.cn
 */
public class StringHash {


    // 将字符串转成hash值
    public static int toHash(String key) {
        int hash = 7;
        for (int i = 0; i < key.length(); i++) {
            hash = hash * 31 + key.charAt(i);
        }
        return hash;
    }

    public static String toHash(String key, int last) {
        int i = toHash(key);
//        System.out.println(i);
        return String.valueOf(i % 1000);
    }

    public static void main(String[] args) {
        for (int i = 0; i < 1000; i++) {
            System.out.println(toHash(String.valueOf(Math.random()), 1));
        }
    }

}
