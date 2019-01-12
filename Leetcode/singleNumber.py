'''
Given a non-empty array of integers, every element appears twice except for one. Find that single one.
Input: [2,2,1]
Output: 1
Input: [4,1,2,1,2]
Output: 4

Hashtable approach  Time O(n) space O(n)
XOR approach : Time O(n) space O(1)

'''


def singleNumber(nums):
    """
    :type nums: List[int]
    :rtype: int
    """
    dict = {}
    for i in nums:
        if i in dict:
            dict.pop(i)
        else:
            dict[i] = 1
    return dict.popitem()[0]

# Using XOR  approach

def singleNumber1(nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        a= 0
        for i in nums:
            a^=i
        return a


print(singleNumber([2,2,1]))
print(singleNumber1([2,2,1]))