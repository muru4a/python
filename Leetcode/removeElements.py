def removeElements(nums,val):
    k = 0      
    while k < len(nums):
        if nums[k] == val:
            #del nums[k]
            nums.pop(k)
            #nums.remove(val)
        else:
            k += 1
    return nums

print(removeElements([3,2,2,3], 3 ))
# print(removeElements1([3,2,2,3], 3 ))
# #print(removeElements([0,1,2,2,3,0,4,2], 2))
print(removeElements([0,1,2,2,3,0,4,2], 2))