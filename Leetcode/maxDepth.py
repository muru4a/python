def maxDepth(s):
    maxi = 0
    curr = 0
    for i in s:
        if i == '(':
            curr+=1
            maxi = max(maxi,curr)
        elif i == ")":
            curr-=1
    return maxi


if __name__ == "__main__":
    print(maxDepth("(1)+((2))+(((3)))"))