# Check palindrome value

def palindrome(s):
    return str(s) == str(s)[::-1]

print (palindrome(122))