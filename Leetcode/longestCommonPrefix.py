def longestCommonPrefix(str):
    result=""
    prefix=str[0]
    if not str or len(str) == 0:
        return ''
    for i in range(1,len(str)):
        j=0
        while j< min(len(str[i]),len(prefix)):
            if str[i][j] != prefix[j]:
               break
            j+=1
        prefix=prefix[:j]
    return prefix

def CommonPrefix(s1, s2):
    out = ''
    for i, j in zip(s1, s2):
        if i != j:
            break
        out += i
    return out



if __name__ == '__main__':
    print(longestCommonPrefix(["flower", "flow", "flight"]))
