def lengthOfLongestSubstring(s):
    dict ={}
    max_so_far = curr_max= start= 0
    for index,i in enumerate(s):                    
        if i in dict and dict[i] >= start :         
            max_so_far = max( max_so_far,curr_max)
            curr_max = index - dict[i]
            start = dict[i]+1
        else:
            curr_max+=1
        dict[i] = index
    return max(max_so_far,curr_max)