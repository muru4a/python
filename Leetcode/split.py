def split(num):
    product=1
    sum=0
    for row in str(num):
        #print(row)
        product *=int(row)
        #print(product)
        sum +=int(row)
        result=product - sum
    return result
    
print(split(234))

    