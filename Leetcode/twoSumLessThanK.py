### two loop approach
def twoSumLessThanK(nums,k):
    result = -1
    for i in range(len(nums)):
        for j in range(i+1,len(nums)):
            sum = nums[i]+nums[j]
            if sum < k:
                result = max(result,sum)
    return result

### Two pointer approach
def twoSumLessThanK1(nums,k):
    nums.sort()
    i=0
    j=len(nums) -1
    result =-1
    while (i<j):
        sum = nums[i] + nums[j]
        if sum <k:
            result = max(result,sum)
            i +=1
        else:
            j-=1
    return result

print(twoSumLessThanK([34,23,1,24,75,33,54,8],60))
print(twoSumLessThanK1([10,20,30],15))

"""

if __name__ == "__main__":
    print(twoSumLessThanK([34,23,1,24,75,33,54,8],60))
    print(twoSumLessThanK1([10,20,30],15))
"""

### Binanry seacrh approach
"""

def twoSumLessThanK2(nums,k):
    nums.sort()
    low =0
    high = len(nums) -1
    mid = 0
    while low <=high:
        mid = (high +low)//2
        sum = nums[low] + nums[high]
        if sum < k and nums[mid] < sum:
            low = mid +1
        else
"""

    

        




