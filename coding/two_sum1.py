def two_sum(nums,target):
    out_dict_seen={}
    for i in nums:
        if (target-i) in out_dict_seen:
            print([nums.index(i)+' , '+nums.index(target-i)])
        else:
            out_dict_seen[i]=1
    return

if __name__ == '__main__':
    two_sum([1,1,3,4,5,8,6,9],10)
