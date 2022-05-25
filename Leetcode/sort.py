def solution(a):
    b= sorted(a)
    result =[]
    if a[0] == b[0] and b[1] == a[-1]:
        result.append(a[0])
        result.append(a[-1])
        for i in range(len(a)):
            result.append(i+1)
            if a[i+1] == b[j+2]:
                    result.insert(i,a[i+1])
    if b == result:
        return True
    else:
        False

print(solution([1, 3, 5, 6, 4, 2]))