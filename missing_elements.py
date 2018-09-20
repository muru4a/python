# Missing Elements in Two Array
def missing_elements(arr1,arr2):
    output=[]
    for i in arr1:
        if i not in arr2:
            output.append(i)
    return output

if __name__=="__main__":
    print(missing_elements([1, 2, 3], [1, 2, 4]))
