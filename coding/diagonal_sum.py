def diagonal_sum(arr):
    sum1=0
    sum2=0
    for i in range(len(arr)):
        for j in range(len(arr[i])):
            if i==j:
               sum1=arr[i][j]+sum1
    print(sum1)
    for i in range(len(arr)):
        sum2+=arr[i][i]
    print(sum2)


if __name__ == '__main__':
    diagonal_sum([[1,2,3],[4,5,6],[7,8,9]])

