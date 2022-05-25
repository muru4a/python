def backspaceCompare(s: str, t: str):
        def build(s):
            result = []
            for i in s:
                if i!= '#':
                    result.append(i)
                elif result:
                    result.pop()
            return "".join(result)
        return build(s) == build(t)

print(backspaceCompare("ab#c","ad#c"))