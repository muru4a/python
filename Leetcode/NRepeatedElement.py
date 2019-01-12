def NRepeatedElement(A):
    counter=set()
    for i in A:
        if i in counter:
            return i
        else:
            counter.add(i)




print(NRepeatedElement([1,2,3,3]))
print(NRepeatedElement([2,1,2,5,3,2]))
print(NRepeatedElement([5,1,5,2,5,3,5,4]))