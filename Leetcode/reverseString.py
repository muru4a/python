'''
# Reverse the String (string is immutable)
# Input : abcde
# output: edcba
'''


def reverseSting(s):
    i=0
    j=len(s)-1
    ss = [''] * len(s)
    while(i<j):
        ss[i]=s[j]
        ss[j]=s[i]
        i+=1
        j-=1
        if i == j:
           ss[i] =s[j]
    return ''.join(ss)


#print(reverseSting("abcde"))


def reverseString2(s):
    if len(s) == 0:
        return s
    else:
        return reverseString2(s[1:]) + s[0]



print(reverseString2("abcde"))
print(reverseSting("abcde"))
