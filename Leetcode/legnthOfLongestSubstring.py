""" Longest Substring Without Repeating Characters
Given a string, find the length of the longest substring without repeating characters.

Example 1:
Input: "abcabcbb"
Output: 3
Explanation: The answer is "abc", with the length of 3.

Example 2:
Input: "bbbbb"
Output: 1
Explanation: The answer is "b", with the length of 1.

Example 3:
Input: "pwwkew"
Output: 3
Explanation: The answer is "wke", with the length of 3.
             Note that the answer must be a substring, "pwke" is a subsequence and not a substring.
"""

"""
Solution :  store in dict , the element and index
         if the value exists in dict and index is greater than start point reset the max count
         else
            increment current max value                   
"""

def lengthOfLongestSubstring(s):
    dict ={}
    max_so_far = curr_max= start= 0
    for index,i in enumerate(s):                    # index=0 i=a -> curr_max =1 , dict[a] = 0
        if i in dict and dict[i] >= start :         # index=1 i=b ->
            max_so_far = max( max_so_far,curr_max)
            curr_max = index - dict[i]
            start = dict[i]+1
        else:
            curr_max+=1
        dict[i] = index
    return max(max_so_far,curr_max)


print(lengthOfLongestSubstring("abcabcbb"))


def lengthOfLongestSubstring1(s):
    dict ={}
    max_length = start= 0
    for i in range(len(s)):
        if s[i] in dict and start <= dict[s[i]]:
            start = dict[s[i]] +1
        else:
            max_length =max(max_length,i-start+1)
        map[s[i]] = i
    return (max_length)


print(lengthOfLongestSubstring1("abcabcbb"))