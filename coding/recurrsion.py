def length_of_list(list):
    if list == []:
        return 0
    return 1 + length_of_list(list[1:])


print(length_of_list([1,2,3,4]))