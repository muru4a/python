'''
Sum of Two Integers
Calculate the sum of two integers a and b, but you are not allowed to use the operator + and -.
Input: a = 1, b = 2
Output: 3
Input: a = -2, b = 3
Output: 1
'''


def getSum(a,b):
    while b:
       a,b =a ^ b ,(a & b) <<1
    return a


print(getSum(2,-13))
