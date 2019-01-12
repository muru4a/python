def larger_than(str1,str2):
    if len(str1)==0 and len(str2)==0:
        return None
    if len(str1)>len(str2):
        print(str1)
    elif len(str1)<len(str2):
        print(str2)
    elif len(str1)==len(str2):
        for i in range(len(str1)):
            if str1[i]==str2[i]:
                continue
            elif str1[i]>str2[i]:
                print(str1)
            else:
                print(str2)



if __name__ == '__main__':
    larger_than("223",'222')
