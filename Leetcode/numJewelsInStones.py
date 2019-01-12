def numJewelsInStones(J,S):
    return sum(i in J for i in S)



if __name__ == '__main__':
    print(numJewelsInStones("aAA","aAAbbbb"))
