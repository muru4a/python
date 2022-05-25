def mergeSortedArray(nums1,nums2):
    i=len(nums1) -1
    j=len(nums2) -1

    list =[]
    while ( i>=0 and j>=0):
        if nums1[i]>= nums2[j]:
            list.append(nums1[i])
            i-=1
        else: 
            list.append(nums2[j])
            j-=1
    while i>=0:
        nums1.remove(nums1[i])
        i-=1
    while j>=0:
        nums2.remove(nums2[j])
        j-=1
    return list

def mergeSortedArray1(nums1,nums2):
    #return nums1+nums2
    print(nums1[:m])
    nums1[:]= sorted(nums1[:m]+nums2)
    return nums1


# nums1 = [1,2,3,0,0,0]
# m = 3
# nums2 = [2,5,6]     
# n = 3
# Output = [1,2,2,3,5,6]

print(mergeSortedArray1([1,2,3,0,0,0],[2,5,6]))




