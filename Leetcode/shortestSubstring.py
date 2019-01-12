''''''
# Find the shortest substring that controls the set
# Input : {abcdbc}, {b,d}
# Output : {db}

# Algorthim Sliding Window approach
# 1. Expand the sliding window right until it reaches the control set
# 2. Reset the left until it reaches the un control set
# 3. Maintain the minimum


def shortesSubstring(s,c):
    freq={}
    l=0
    r=0
    min_len=0
    min_st=-1
    min_end=-1
    rec=set()
    while r<len(s):
        # Expand the sliding window until it reaches the control set
        while len(freq) != len(c):
            if s[r] not in freq:
                freq[s[r]] =0
            #freq[s[r]] +=1
            rec.add(s[r])
        r+=1
        if r == len(s):
               break

    # Reset the left until it reaches the un control set
    while l<=r and len(freq) == len(c):
        if s[l] in c:
            freq[s[l]]-=1
            if freq[s[l]] == 0:
                #del freq[s[l]]
                rec.remove(s[l])
        l+=1
    # Maintain the Minimum values
    if r-l+2 < min_len:
        min_len=r-l+2
        min_st=l-1
        min_end=r
    return s[min_st:min_end]


print(shortesSubstring("abcdbc","bd"))






