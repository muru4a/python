from audioop import reverse


def twosum1(nums,k):
    for index,val in enumerate(nums):
        if k -val in nums[index+1:]:
            return [index, nums[index+1:].index(k-val)+index+1]
        

def twosum2(nums,k):
    dict = {}
    for index,val in enumerate(nums):
        if k -val in dict:
            return dict[k-val],index
        dict[val]=index

print(twosum1([2, 11, 7, 15],9))
print(twosum2([2, 11, 7, 15],9))

### dict sort
dict = {0: 10, 2: 30, 1: 20}
sort_dict= sorted(dict.items())
print(sort_dict)

## dict add 
dict.update({3:40})
print(dict)
dict2 = dict.copy()
print(dict2)
## concantentate the dict
dic1={1:10, 2:20}
dic2={3:30, 4:40}
dic3={5:50,6:60}

dict_final = dic1.zip(dic2)
print(dict_final)
#dict_final = {***dic1,***dic2,***dic3}