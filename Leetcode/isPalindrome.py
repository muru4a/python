def isPalindrome(x):
    """
    :type x: int
    :rtype: bool
    """
    reversed = 0
    remainder = 0
    number =x
    if (x==0):
        return True
    if (x<0 or x %10 == 0):
        return False
    while (x != 0):
        remainder = x % 10
        print(remainder)
        reversed = reversed * 10 + remainder
        x =x // 10
        print(reversed)
    if reversed == number:
        return True
    else:
            return False



#print(isPalindrome(-121))


def palindrome2(s):
    i=0
    j=len(s)-1
    while(i >=0 and j<len(s) and s[i] == s[j]):
        i+=1
        j-=1
        if i == j:
            return True
    return False


print(palindrome2('racecar1'))