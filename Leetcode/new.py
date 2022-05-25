def new(j):
    k=0
    while j>0:
        k= 10 * k +j %10
        j = j //10
    return k

if __name__ == "__main__":
    print(new(1230))
    print(type([1,15,"hello","30"]))