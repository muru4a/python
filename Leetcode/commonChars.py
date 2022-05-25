def commonChars(words):
        result =[]
        for i in words[0]:
            if all( i in j for j in words):
                result.append(i)
                words = [k.replace(i,'',1) for k in words]
        return result
if __name__ == "__main__":
    print(commonChars(["bella","label","roller"]))
