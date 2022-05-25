def sumOfUnique(nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        dict ={}
        result =0
        for row in nums:
            if row in dict:
                dict[row] +=1
            else:
                dict[row]=1
        for k , v in dict.items():
            if v == 1:
                result +=k
        return result 

if __name__ == "__main__":
    print(sumOfUnique([1,2,3,2]))