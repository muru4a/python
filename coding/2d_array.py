def two_array(arr):
    for row in arr:
        for item in row:
            print(item)
    for i in range(len(arr)):
        for j in range(len(arr[i])):
            print(arr[i][j])



if __name__ == '__main__':
    two_array([[1,1,3,4],[3,4,5,6]])
