def merge_dict(dict1,dict2):
    #return (dict2.update(dict1))
    res = {**dict1, **dict2}
    return res


dict1 = {'a': 10, 'b': 8}
dict2 = {'b': 8, 'c': 4}
print(merge_dict(dict1,dict2))
print(dict2)


