def fizzBuzz(n):
    # Write your code here
    nums = []
    for num in range(1, n+1):
        three_mul = False
        five_mul = False
        if num%3==0:
            three_mul = True
        if num%5==0:
            five_mul = True
        if three_mul and five_mul:
            print('FizzBuzz'.strip('"\''))
        elif three_mul:
            #num='Fizz'
            print('Fizz'.strip('"\''))
        elif five_mul:
             #num='Buzz'
            print('Buzz'.strip('"\''))
        else:
            print(num)
    return num





print(fizzBuzz(15))