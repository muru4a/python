def isAnagram(nums1,nums2):
    if len(nums1) != len(nums2):
        return "Not valid"
    dict1={}
    for i in nums1:
        if i in dict1:
            dict1[i]+=1
        else:
            dict1[i]=1
    dict2={}
    for i in nums2:
        if i in dict2:
            dict2[i]+=1
        else:
            dict2[i]=1
    
    if  dict1 == dict2:
       return True
    return False

s = "anagram"
t = "fagaram"
print(isAnagram(s,t))
