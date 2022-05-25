def isPalindrome(x):
    if x==0:
        return True
    if x < 0 or x %10 == 0:
        return False
    revert = 0
    prev = x
    while x > revert:
        pop = x %10 
        prev = x
        x /=10
        revert =revert *10 + pop
    if (x == revert or prev == revert):        return True
    else:
        return False

if __name__ == '__main__':
    print(isPalindrome(111))