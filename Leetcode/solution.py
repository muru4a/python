def solution(A):
    # write your code in Python 3.6
    dict ={}
    for i in A:
        if i in dict:
            dict[i]+=1
        else:
            dict[i]=1
    result = []
    for key,val in dict.items():
        if val == 1:
            result.append(key)
        
    return result



print(solution([4, 10, 5, 4, 2, 10]))
