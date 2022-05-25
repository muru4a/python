def isPerfectSquare(nums):
    if nums < 2:
        return True
    left,right = 2,nums//2
    while left<= right:
        x= left + (right- left)//2
        square = x * x
        if square == nums:
            return True
        if square > nums:
            right=x-1
        else:
            left=x+1
    return False

print(isPerfectSquare(16))