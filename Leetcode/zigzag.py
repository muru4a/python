def solution(numbers):
    result = []
    for i in range(len(numbers)-2):
        if numbers[i] < numbers[i+1] >  numbers[i+2]:
           result.append(1)    
        elif numbers[i] > numbers[i+1] < numbers[i+2]:
            result.append(1)
        elif numbers[i] < numbers[i+1] < numbers[i+2]:
            result.append(0)
    return result


print(solution([1, 2, 1, 3, 4]))
print(solution([1, 2, 3, 4]))
