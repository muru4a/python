'''
Given an array of charscters print all the subsets of an array
Input: tdco  output : ___,t,d,c,

'''

def subsetCombinations(a,i,s,j):
    if i == len(a):
        print(a)
    else:
        subsetCombinations(a,i+1,s,j)
        s[j]= a[i]
        subsetCombinations(a,i+1,s,j+1)
    return


# Test the function
a="tdco"
s="dco"

print(subsetCombinations(list(a),0,list(s),0))

