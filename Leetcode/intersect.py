# Intersect Two Arrays Sorted and unsorted
# Input: nums1 = [1,2,2,1], nums2 = [2,2]
# Output : [2,2]
# Algorthim BF: Check each elements by num2. Time complexity O(mn) Space : 0(1) constant
# 1. Index i,j on each array and comparing the values and increment the index values if it lesser. Time complexity: O(m+n) Space: 0(1) constant
# 2. Use Hashmap approach work for sorted and unsortedd  Push one array into hashmap and loop through find out the common elements Time complexity: O(N) Space: 0(N) constant
# 3. Use collection counter standard liberary


def intersect(nums1, nums2):
    """
    :type nums1: List[int]
    :type nums2: List[int]
    :rtype: List[int]
    """
    i=0
    j=0
    result=[]
    while(i<len(nums1) and j<len(nums2)):
        if nums1[i] == nums2[j]:
            result.append(nums1[i])
            i+=1
            j+=1
        elif nums1[i] < nums2[j]:
            i+=1
        else:
            j+=1
    return result

def intersect1(num1, num2):
    result=[]
    dict={}
    for i in num1:
        dict[i]=dict.get(i,0)+1
    for i in num2:
        if i in dict and dict[i] >0:
            result.append(i)
            dict[i]-=1
    return result

import collections
def intersect2(num1,num2):
    counts = collections.Counter(num1)
    result=[]

    for i in num2:
        if counts[i] >0 :
            result +=i,
            counts[i]-=1
    return result





print(intersect([4,9,5],[9,4,9,8,4]))
print(intersect1([4,9,5],[9,4,9,8,4]))
print(intersect2([1,2,2,1],[2,2]))