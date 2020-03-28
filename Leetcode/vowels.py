
def removeVowels(S):
        """
        :type S: str
        :rtype: str
        """
        vowels =['a','e','i','o','u']
        output =[]
        for row in S:
            if row not in vowels:
                output.append(row)
        return "".join(map(str,output))



print (removeVowels( "leetcodeisacommunityforcoders"))


