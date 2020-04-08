"""
Given an array nums of n integers, are there elements a, b, c in nums such that a + b + c = 0? Find all unique triplets in the array which gives the sum of zero.

Note:
The solution set must not contain duplicate triplets.

Example:
Given array nums = [-1, 0, 1, 2, -1, -4],
A solution set is: [ [-1, 0, 1],  [-1, -1, 2]]

Solution:
1. sort the array asc [-4,-1,-1,0,1,2]
2. i=0 first element i=-4  L        R
3. assign two pointer  L , R
L-->i+1 R-> last element
4.sumtotal = i + L + R

   if sumtotal == 0
     L=L+1
     R=R-1
     result.append([nums[i],nums[L],nums[R]])
   if sumtotal < 0 :
    L=L+1
   if sumtotal > 0 :
   R=R-1

   check on i value duplucates and ignore the values
   check on L,R vaLue duplicates and move on next value

"""

def threeSum(nums):
    result =[]
    nums.sort()
    length = len(nums)
    for i in range(length -2):
        if i>0 and nums[i] == nums[i -1]:
            continue
        l = i+1
        r = length -1
        while(l<r):
            total = nums[i] + nums[l] + nums[r]
            if total < 0:
                l=l+1
            elif total > 0:
                r=r-1
            else:
                result.append([nums[i],nums[l],nums[r]])
                while l<r and nums[l] == nums[l+1]:
                    l=l+1
                while l<r and nums[r] == nums[r-1]:
                    r = r-1
                l=l+1
                r=r-1
    return result


print(threeSum([-1, 0, 1, 2, -1, -4]))






