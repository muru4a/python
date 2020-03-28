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


#print(findDuplicate([1,3,4,2,2]))


def find_duplicate(nums):
    result=[]
    counter=0
    for i in nums:
        if i in result:
            counter = counter+1
            if counter > 1:
                return i
        else:
            result.append(i)



print(find_duplicate([1,3,4,2,2]))


#input -- Array(1,2,3,1,4,3,6,8)
#
# output --
#
#
# find out duplicates -
#
#     1 - 2
#     3 - 2


def find_count_dups(nums):
    flag = False
    result = []
    for i in nums:
        if i in result:
            flag =True
            continue
        else:
            counter = 0
            for j in nums:
                if i == j:
                    counter =counter +1
            if counter >1 :
                return (i,counter)
            result.append(i)
    if flag == False :
        print("no duplicates")

print(find_count_dups([1,2,3,1,4,3,6,8]))


def find_count_dups1(nums):
    result = {}
    #result = {x:nums.count(x) for x in nums}
    for i in nums:
        result[i] = nums.count(i)
    for key,value in result.items():
        return (key,value)

print(find_count_dups1([1,2,3,1,4,3,6,8]))









