def removeNumeric(s):
    words_list =list(s)
    result=[]
    for i in words_list:
        if not i.isdigit():
            result.append(i)
    return ''.join(result)

print(removeNumeric("abb24ba2g6v9"))