"""
# 4: Longest Substring Without Repeating Characters
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

Solution:

sliding window techinque

"abcabcbb"


"""

def longestSubstring(s):

    if len(s) == 0:
        return 0
    dict = {}
    max_length = start = 0
    for i in range(len(s)):
        if s[i] in dict and start <= dict[s[i]]:
            start =  dict[s[i]] + 1
        else:
            max_length =max(max_length,i-start+1)
        dict[s[i]] = i
    return (max_length)


print(longestSubstring("abcabcbb"))



