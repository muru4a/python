def missingNumber(nums):
        nums = set(nums)
        n = len(nums)+1
        for i in  range(n):
            if i not in nums:
                return i
if __name__ == "__main__":
    print(missingNumber([3,3,0,1]))