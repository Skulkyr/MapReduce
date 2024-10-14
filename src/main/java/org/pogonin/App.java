package org.pogonin;

import org.pogonin.mapReduce.MapReduce;

import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        List<String> tasks = List.of(
                "first.txt",
                "second.txt",
                "threed.txt"
        );
        String directory = "G:/test/";
        MapReduce mapReduce = new MapReduce(3);
        mapReduce.execute(tasks, directory);
//        List<String> words = List.of(
//                "яблоко",
//                "дерево",
//                "морковь",
//                "компьютер",
//                "река",
//                "горизонт",
//                "телефон",
//                "музыка",
//                "велосипед",
//                "книга",
//                "солнце",
//                "ночь",
//                "зима",
//                "карандаш",
//                "путешествие",
//                "машина",
//                "облако",
//                "лес",
//                "камень",
//                "звезда"
//        );
//
//        for (String word : words) {
//            System.out.println(getBucketNumber(word));
//        }
    }
//
//    private static int getBucketNumber(String input) {
//        return (Math.abs(input.hashCode() << 2) % 3);
//    }
}
