def reverse_string(a_string):
    new_strings = []
    index = len(a_string)
    while index:
        index -= 1
        new_strings.append(a_string[index])
    return ''.join(new_strings)

if __name__=="__main__":
    print(reverse_string("abcd"))