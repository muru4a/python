def rooks_are_safe(chessboard):
    n = len(chessboard)
    print(n)
    for row in range(n):
        row_count = 0
        for col in range(n):
            row_count += chessboard[row][col]
        if row_count > 1:
            return False
    for col in range(n):
        col_count = 0
        for row in range(n):
            col_count += chessboard[row][col]
        if col_count > 1:
            return False
    return True


rooks_are_safe([[0,1,0,0],
                [0,0,1,0],
                [0,0,0,0],
                [0,0,0,1]])


