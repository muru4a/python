'''
# Top K Frequent Elements
# Given a non-empty array of integers, return the k most frequent elements.
# Input: nums = [1,1,1,2,2,3], k = 2
# Output: [1,2]
# Hash map and method Counter in the collections library O(nlogk) Time O(n) space
# Heap method
# Dictionary approach
'''


from collections import Counter
def topKFrequent( nums, k):

    """
    :type nums: List[int]
    :type k: int
    :rtype: List[int]
    """
    counted=Counter(nums)
    most_common_elements=counted.most_common(k)
    return [pair[0] for pair in most_common_elements]

def topK(nums,k):
    dict={}
    for i in nums:
        dict[i]=dict.get(i,0)+1
    ans=sorted(dict.keys(),key=lambda x: dict[x], reverse=True)
    return ans[:k]


if __name__ == '__main__':
    print(topKFrequent([1, 1, 1, 2, 2, 3], 2))
    print(topK([1, 1, 1, 2, 2, 3], 2))



