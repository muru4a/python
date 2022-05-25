def countNegatives(grid):
    """
    :type grid: List[List[int]]
    :rtype: int
    """
    result = 0
    for row in grid:
        for col in row:
            if col< 0:
                result +=1
    return result

if __name__ == "__main__":
    print(countNegatives([[3,2],[1,0]]))
    print(countNegatives([[4,3,2,-1],[3,2,1,-1],[1,1,-1,-2],[-1,-1,-2,-3]]))