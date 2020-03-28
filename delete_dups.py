def delete_dups(arr):
    result= []
    for row in arr:
        if row not in result:
            result.append(row)
    return result



print(delete_dups([1,-1,2,3,2,3,5,7]))