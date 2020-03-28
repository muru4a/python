class Solution(object):
    def twoSum(self, nums, target):
        self.nums=nums
        self.target=target
        for i,num in enumerate(nums):
            k=target-num
            if k in nums[i+1:]:
                return [i,nums.index(k,i+1)]
        return None

   # p1 = Solution([1, 1, 3, 4, 5, 8, 6, 9], 10)

if __name__ == '__main__':
    p1=Solution([1,1,3,4,5,8,6,9],10)
    print(p1)
