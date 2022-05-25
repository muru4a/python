def twoSum(num,target):
    dict = {}
    for index,row in enumerate(num):
        if target - row in dict:
            return dict[target -row],index
        dict[row]=index

if __name__ == '__main__':
    print(twoSum([2,7,11,15],9))