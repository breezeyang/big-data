package com.breeze.mr_demo.commonfriend;

import java.util.Set;
import java.util.TreeSet;

public class CommonFriend {

    public static Set<Integer> intersection(Set<Integer> user1friend, Set<Integer> user2friend) {
        if (user1friend == null || user1friend.isEmpty()) {
            return null;
        }
        if (user2friend == null || user2friend.isEmpty()) {
            return null;
        }
        if (user1friend.size() < user2friend.size()) {
            return intersect(user1friend, user2friend);
        } else {
            return intersect(user2friend, user1friend);
        }
    }

    private static Set<Integer> intersect(Set<Integer> smallSet, Set<Integer> largeSet) {
        Set<Integer> result = new TreeSet<Integer>();
        for (Integer x : smallSet) {
            if (largeSet.contains(x)) {
                result.add(x);
            }
        }
        return result;
    }
}
