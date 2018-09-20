# Check the common elements in two arrays

def common_elements(list1,list2):
    output=[]
    for i in list1:
        if i in list2:
            output.append(i)
    return output

# Check the common elements in sorted arrays
def common_elements1(list1,list2):
    p1=0
    p2=0
    result=[]
    while p1<len(list1) and p2 < len(list2):
        if list1[p1]==list2[p2]:
            result.append(list1[p1])
            p1=p1+1
            p2=p2+1
        elif list1[p1]>list2[p2]:
            p2+=1
        else:
            p1+=1
    return result

if __name__=="__main__":
    print(common_elements([12,2,4,6],[3,4,2]))
    print(common_elements1([12,2,4,6],[3,4,2]))