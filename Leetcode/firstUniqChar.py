'''
Given a string, find the first non-repeating character in it and return it's index. If it doesn't exist, return -1.
s = "leetcode"
return 0.

s = "loveleetcode",
return 2.

BF : Go thougth all the elements in the array  Time 0(n*n) Space O(n)
Hashmap to store the key and value Time O(n) Space O(n)


'''

import collections

def firstUniqChar(s):
    """
    :type s: str
    :rtype: int
    """
    l=list(s)
    dict=collections.Counter(l)
    for i,v in enumerate(s):
        if(dict[v] == 1):
            return i
    return -1


print(firstUniqChar("loveleetcode"))
print(firstUniqChar("leetcode"))



