'''
Given array of string print all the permutation combination
Recursion approach
Input : bcef
output : 4!= 4*3*2*1 = 24 Time : O(n*n!) space O(n)
'''

def permutation(s,l,r):
    # Base Class on recursion
    if l == r:
        print(s)
    else:
        for i in range(l,r+1):
            #swap the elements
            s[l],s[i] =s[i],s[l]
            permutation(s, l+1, r)
            #swap and bring to orginal position
            s[l],s[i] = s[i],s[l]



# Test the String

s="bcef"
n=len(s)
print(permutation(list(s),0,n-1))

