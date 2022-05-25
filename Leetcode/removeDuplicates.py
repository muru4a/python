def removeDuplicates(s: str) -> str:
    """
    use the stack 
    check the element equal to last element in stack and pop the element
    current string character not equal to last element in stack and add the element in stack
    """
    result = []
    for row in s:
        if result and row == result[-1]:
            result.pop()
        else:
            result.append(row)
    return ''.join(result)

if __name__ == "__main__":
    print(removeDuplicates("abbaca"))