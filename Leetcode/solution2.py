def solution2(S):
    l= 0
    r= len(S)-1
    while l<r:
        if S[l] == S[r]:
            l+=1
            r-=1
        else:
            return 0
    return l

print(solution2("racecare"))