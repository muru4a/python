# Longest Palinfrome Substring
# Input : abcbabcbcdddd
# output: cbabc

def LongestPalindrome(s):
    left=0
    right=0
    for i in range(len(s)):
        len1=expandaroundcenter(s,i,i)
        len2=expandaroundcenter(s,i,i+1)
        lenval=max(len1,len2)
        if (lenval >right-left):
            left=i-(lenval-1)/2
            right=i+lenval/2
    return s[left:right+1]

def expandaroundcenter(s,left,right):
    while (left>=0 and right<len(s) and s[left] == s[right]):
        left=left-1
        right=right+1
    return right-left-1



if __name__ == '__main__':
    print(LongestPalindrome("abcbabcbcdddd"))
    print (LongestPalindrome("babad"))
    print(LongestPalindrome("cbbd"))

