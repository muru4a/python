def canBeEqual(target, arr):
        """
        :type target: List[int]
        :type arr: List[int]
        :rtype: bool
        """
        
        dict1={}
        dict2={}
        for row in target:
            if row in dict1:
                dict1[row] +=1
            else:
                dict1[row]=1
        for row in arr:
            if row in dict2:
                dict2[row] +=1
            else:
                dict2[row]=1
                
        if dict1 == dict2:
            return True
        else:
            return False

if __name__ == "__main__":
    print(canBeEqual([1,2,3,4],[2,4,1,3]))
            

            