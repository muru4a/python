def defangIPaddr(address: str):
    result = []
    for i in address:
        if i == ".":
            result.append("[.]")
        else:
            result.append(i)
    return "".join(map(str,result))


if __name__ == "__main__":
    print(defangIPaddr("1.1.1.1"))
    