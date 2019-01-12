def is_leap(year):
    leap = False
    if (year % 400 ==0) or (year%100 != 0 ) and (year % 4 == 0) :
        return True
    return leap


print(is_leap(2100))
