package com.breeze.mr_demo.combination;


import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;


public class Combination {


    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {
        List<List<T>> result = new ArrayList<List<T>>();
        for (int i = 0; i <= elements.size(); i++) {
            result.addAll(findSortedCombinations(elements, i));
        }
        return result;
    }

    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<List<T>>();

        if (n == 0) {
            result.add(new ArrayList<T>());
            return result;
        }

        List<List<T>> combinations = findSortedCombinations(elements, n - 1);//假设已经生成了n-1阶的唯一组合集合
        for (List<T> combination : combinations) {
            //对于n-1阶的唯一组合集合中的每一个集合
            for (T element : elements) {
                //对于给定组合中的每一个元素
                if (combination.contains(element)) {
                    continue;
                }
                //如果n-1阶的唯一组合集合中的这个集合中不包含这个元素，证明这个元素可以跟这个集合一起构成n阶唯一组合集合中的一个元素
                List<T> list = new ArrayList<T>();
                list.addAll(combination);

                if (list.contains(element)) {
                    continue;
                }

                list.add(element);
                //sort items not to duplicate the items
                //   example: (a, b, c) and (a, c, b) might become
                //   different items to be counted if not sorted
                Collections.sort(list);

                if (result.contains(list)) {
                    continue;
                }

                result.add(list);
            }
        }
        return result;
    }

    /**
     * Basic Test of findSortedCombinations()
     *
     * @param args
     */
    public static void main(String[] args) {
        List<String> elements = Arrays.asList("a", "b", "c", "d", "e");
        List<List<String>> combinations = findSortedCombinations(elements, 2);
        System.out.println(combinations);
    }

}
