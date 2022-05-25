def removeDuplicates(nums): 
        i=0
        j=1
        n =len(nums)-1
        while n>0:
            if nums[i] == nums [j]:
                del nums[i]
            else:
                i+=1
                j+=1
            n-=1
        return len(nums)
        
if __name__ == "__main__":
    print(removeDuplicates([0,0,1,1,1,2,2,3,3,4]))

