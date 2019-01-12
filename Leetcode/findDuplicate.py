'''
Given an array nums containing n + 1 integers where each integer is between 1 and n (inclusive),
prove that at least one duplicate number must exist. Assume that there is only one duplicate number, find the duplicate one.

Input: [1,3,4,2,2]
Output: 2

Input: [3,1,3,4,2]
Output: 3

BF: Go through the array on each elements Time O(n*n) Space O(n)
HAshtable Approach  :  Time O(n) Space O(1)

'''


def findDuplicate( nums):
    """
    :type nums: List[int]
    :rtype: int
    """
    nums_set = set()
    for i in nums:
        if i in nums_set:
            return i
        else:
            nums_set.add(i)


print(findDuplicate([1,3,4,2,2]))
