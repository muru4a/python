def sequence(nums):
    result=[]
    for row in range(1,len(nums),2):
        temp = [nums[row]*nums[row -1]]
        result.extend(temp)
    return result



print(sequence([1,2,3,4]))