def toLowerCase(str):
    Upercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    Lowercase = "abcdefghijklmnopqrstuvwxyz"

    HashTable = dict(zip(Upercase, Lowercase))
    print(HashTable)

    LowerStr = ""
    for c in str:
        if c not in HashTable.keys():
            LowerStr += c
        else:
            LowerStr += HashTable[c]
    return LowerStr

print(toLowerCase('ABC'))