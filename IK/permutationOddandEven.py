'''
Assume that the input is an array of size 'n' where 'n' is an even number. Additionally,
assume that  half the integers are even and the other half are odd.
Print only those permutations where odd and even integers alternate, starting with odd.
Approvcah : Recursion Time:O(n*n!) Space 0(n)

'''

def permutationOddandEven(a,l,r):
    if l==r:
        print(a)
    else:
        for i in range(l,r+1):
            if ((a[i] % 2 == 0 and i % 2 == 1) or (a[i] % 2 == 1 and i % 2 == 0)):
                # swap the elements
                a[l],a[i] = a[i],a[l]
                permutationOddandEven(a,l+1,r)
                # swap the elements
                a[l], a[i] = a[i], a[l]
    return


# Test the function
a=[1,2,3,4,5,6]
n=len(a)
print(permutationOddandEven(a,0,n-1))
