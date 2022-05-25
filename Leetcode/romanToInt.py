def romanToInt(s):
    """
        :type s: str
        :rtype: int
        approach: right to left pass 
        Time complexity : O(1)
        Space complexity : O(1)
    """
    values = {
                "I": 1,
                "V": 5,
                "X": 10,
                "L": 50,
                "C": 100,
                "D": 500,
                "M": 1000,
            }

    total = values.get(s[-1])
    for i in reversed(range(len(s)-1)):
        if values[s[i]] < values[s[i+1]]:
            total -=values[s[i]]
        else:
            total +=values[s[i]]
    return total

if __name__ == '__main__':
    print(romanToInt('III'))

        
    