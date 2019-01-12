'''
# Give sorted array identify pair of elements add up to K
# Input : {1,1,2,3,4,5,6} k=10
# Output : {6,4}
# BF : Going through the array for differences O(n+mn) Time O(1)
# For sorted Array : Two pointer approach i,j go through the array left and right and break the loop after it cross O(n) Time O(1) Space
# Unsorted Array : Using Hash Map to keep track the difference and indices. O(n) Time O(n) space
'''

def twosum(arr,k):
    i=0
    j=len(arr)-1
    while(i<j):
        if arr[i]+arr[j] < k:
            i+=1
        elif arr[i]+arr[j] > k:
            j-=1
        else:
            break
    return (i, j)

def twosum1(arr,k):
    if len(arr) <=1:
        return None
    dict={}
    for i in arr:
        if k-i in dict:
            return ([arr.index(i),arr.index(k-i)])
        else:
            dict[i]=1
    return




#print(twosum((1,1,2,3,4,5,6),6))
#print(twosum([1,1,3,4,5,8,6,9],12))
print(twosum1([1,3,11,2,7],9))

