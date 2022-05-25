def targetIndices(nums, target):
        #sort=sorted(nums)
        nums.sort()
        result = []
        for index,val in enumerate(nums):
            if  val == target:
                result.append(index)
                
        return result

def targetIndices1(nums,target):
    index = 0
    count = 0

    for num in nums:
        if num < target:
            index+=1
        if num == target:
            count+=1
    return list(range(index,index+count))


if __name__ == "__main__":
    print(targetIndices([1,2,5,2,3],2))
    print(targetIndices([1,2,5,2,3],3))
    print(targetIndices1([1,2,5,2,3],2))
    print(targetIndices1([1,2,5,2,3],3))