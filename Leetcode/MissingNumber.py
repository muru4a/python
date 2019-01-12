'''
Find the Missing Number in the list
BF: Go through each elements after its sorted Time O(nlogn) Space O(n) for size copy
Hashmap approach : Use set store the data and check array once in linear. Time O(n) Space O(n) for storing in set
'''


def missingNumber(nums):
    """
    :type nums: List[int]
    :rtype: int
    """
    num_set = set(nums)
    missing = 1
    for i in range(1, len(nums) + 1):
        if i not in num_set:
            missing = i
    return missing


print(missingNumber([3,5,6,2,1,1]))

