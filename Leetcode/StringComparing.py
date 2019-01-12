'''
a ='g<>ogle' == b = "google"
google
g<>ogle
go<>ogle
g<><>gle
'''


def stringcompare(a,b):
    if len(a) < len(b):
        return None
    i=0
    j=0
    while(i<len(a) and  j <len(b)):
        if a[i] == b[j]:
            i+=1
            j+=1
        elif a[i:i+2] == '<>' or b[j] == 'o':
             i+=2
             j+=1
        elif i == j:
            break
        else:
            return False
    return True

print(stringcompare('go<>gle','google'))

