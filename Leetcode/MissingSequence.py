'''
# Find the missing sequence on the array for indices on i and j values on sorted arrays
# Input : [1,2,3,4,5,7,9,10] i=4 and j=7
# Hashmap approach  O(n) Space O(n)
#Sequence difference as counter. loop through start and end
'''

def MissingSequence(arr):
    freq={}
    dup=-1
    missing=1
    for i in range(len(arr)):
        freq[arr[i]]=freq.get(arr[i],0)+1
        if freq[arr[i]] == 2:
            dup=arr[i]
    for i in range(1,len(arr)+1):
        if i not in freq:
            missing=i
    return [dup,missing]

print(MissingSequence([1,2,2,4]))






