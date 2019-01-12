'''
# [1,3,5,7,9], 3 => 1
# [1,3,5,7,9], 7 => 3
# [1,3,5,7,9], 0 => -1
# [1,3,5,7,9], 4 => -1
BF: O(n) --Linear approach
best: Binary Search - Iterativly divide half find the index. Time O(logn) Space O(1
'''


def findindex(a,l,r,num):
    l=0
    r=len(a)-1
    if (l == r):
        return l
    while(l<=r):
        mid =l+(r-1)/2
        #Check num present at mid
        if a[mid]==num:
            return mid
        #Ignore the right half since num less than mid
        elif num<a[mid]:
            r=mid-1
        #Ignore the left half since num is greater than mid
        elif num> a[mid]:
            l=mid+1
    return -1


a=[1,3,5,7,9]
l=0
r=len(a)-1
print(findindex(a,l,r,1))






