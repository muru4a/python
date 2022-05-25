def arraysIntersection(arr1, arr2, arr3):
    p1= 0
    p2 = 0
    p3 = 0
    ans = []
    while p1 < len(arr1) and p2 <len(arr2) and p3<len(arr3):
        if arr1[p1] == arr2[p2] == arr3[p3]:
            ans.append(arr1[p1])
            p1+=1
            p2+=1
            p3+=1
        else:
            if arr1[p1] < arr2[p2]:
                p1+=1
            elif arr2[p2] < arr3[p3]:
                p2+=1
            else:
                p3+=1
    return ans

if __name__ == '__main__':
    print(arraysIntersection([1,2,3,4,5],[1,2,5,7,9],[1,3,4,5,8]))