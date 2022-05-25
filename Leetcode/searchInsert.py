def searchInsert(nums,target):
    for k,v in enumerate(nums):
        if v == target:
            return k
        elif k==len(nums)-1 and v < target: 
            return len(nums)
        elif v > target:
            return k
            

print(searchInsert([1,3,5,6], 0 ))