def secondLargest(arr):
    if len(arr) == 0:
        return None
    if len(arr)<=1:
        return None
    largest=None
    second_largest=None
    for curr_number in arr:
        if largest == None:
            largest=curr_number
        elif curr_number>largest:
            second_largest = largest
            largest=curr_number
        elif second_largest == None:
             second_largest=curr_number
        elif curr_number>second_largest:
             second_largest=curr_number
    return second_largest

if __name__ == '__main__':
    print(secondLargest([1,3,4,5,6]))










