def sortArrayByParity(nums):
    """
    :type nums: List[int]
    :rtype: List[int]
    """
    even =[]
    odd = []
    for row in nums:
        if row% 2 == 0:
            even.append(row)
        else:
            odd.append(row)
    return even+odd

if __name__ == "__main__":
    print(sortArrayByParity([3,1,2,4]))