def palindrom(s):
    if str(s) == s[::-1]:
        return True

def palindrom1(s):
    i= 0
    j =len(s)-1
    while (i<j):
        if s[i] != s[j]:
            return False
            i+=1
            j-=1
    return True



#print(palindrom1('aa'))
print(palindrom('aa'))

