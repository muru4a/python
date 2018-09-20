def anagram1(s1,s2):
    s1=s1.replace(' ','').lower()
    s2=s2.replace(' ','').lower()
    #edge case
    if len(s1)!=len(s2):
        return False
    count={}
    for i in s1:
        if i in count:
            count[i]+=1
        else:
            count[i]=1
    for i in s2:
        if i in count:
            count[i]-=1
        else:
            count[i]=1
    for k in count:
        if count[k]!=0:
            return False
    return True

if __name__=="__main__":
    print(anagram1('abc','cba'))