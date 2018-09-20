#reverse interger
def reverse(s):
    if s < 0:
        return -reverse(-s)
    result=0
    while (s>0):
        result=result*10+s%10
        s//=10
    return result if result <= 0x7fffffff else 0


if __name__ == "__main__":
    print(reverse(456))
