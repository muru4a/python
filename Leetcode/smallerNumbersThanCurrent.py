def smallerNumbersThanCurrent(nums):
    """
    :type nums: List[int]
    :rtype: List[int]
    """
    result = {}
    count = 0
    for row in sorted(nums):
        if row not in result:
            result.update({row: count})
            count += 1
        else:
            count += 1
    return (result[i] for i in nums)

print(smallerNumbersThanCurrent([8,1,2,2,3]))