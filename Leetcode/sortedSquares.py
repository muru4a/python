def sortedSquares(nums):
    """
    :type nums: List[int]
    :rtype: List[int]
    """
    n= len(nums)
    result =[0]*n
    i=0
    j=n-1
    if len(nums) == 1:
        return [0]
    for row in range(n-1,-1,-1):
        if abs(nums[i]) < abs(nums[j]):
            square = nums[j]
            j-=1
        else:
            square = nums[i]
            i+=1
        result[row] = square*square
    return result

if __name__ == "__main__":
    print(sortedSquares([-4,-1,0,3,10]))