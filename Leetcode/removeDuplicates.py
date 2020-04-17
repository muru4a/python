"""
Given nums = [1,1,2],

Your function should return length = 2, with the first two elements of nums being 1 and 2 respectively.

It doesn't matter what you leave beyond the returned length.

Given nums = [0,0,1,1,1,2,2,3,3,4],

Your function should return length = 5, with the first five elements of nums being modified to 0, 1, 2, 3, and 4 respectively.

It doesn't matter what values are set beyond the returned length.

Solution:



"""

def removeDuplicates(nums):
    result=[]
    for i in nums:
        if i not in result:
            result.append(i)
    print(result)
    return len(result)

from itertools import groupby
def removeDuplicates1(nums):
    res = [i[0] for i in groupby(nums)]
    print(res)
    return len(res)

def removeDuplicates2(nums):
    if not nums:
        return 0
    i=0
    j=1
    n=len(nums)-1
    while n >0:
        if nums[i] == nums[j]:
            del nums[i]
        else:
            i+=1
            j+=1
        n-=1
    return len(nums)

import collections
def removeDuplicates3(nums):
    if not nums:
        return 0
    count =  collections.Counter(nums)
    nums [:]= [i for i in count]
    return len(nums)

def removeDuplicates4(nums):
    index=1
    while index < len(nums):
        if nums[index] == nums [index -1]:
            del nums[index]
        else:
            index +=1
    return len(nums)

print(removeDuplicates1([1,1,2]))
print(removeDuplicates([1,1,2]))
print(removeDuplicates2([1,1,2]))
print(removeDuplicates3([1,1,2]))
print(removeDuplicates3([1,1,2]))