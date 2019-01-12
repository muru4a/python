'''
nums1 = [1,2,3,0,0,0], m = 3
nums2 = [2,5,6],       n = 3

Output: [1,2,2,3,5,6]
Merge Sorted approach - Time : O(nlogn) Space : O(n)

3 pointer approach for to move the elements on comparing each

i=m-1
j=n-1
k=m+n-1

1. compare the elements first array i,k and replace the elements on 0th position if i>k move the pointers to k=-1, j=-1
2. compare the j the element with k  if i=k then move pointers k=-1 ,j=-1


'''

def mergesort(nums1,nums2):
    i=len(nums1)-1
    j=len(nums2)-1
    k=len(nums1)+len(nums2)-1
    while (i<j and )