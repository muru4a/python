def order_book(orders):
    output = 0
    buy = {}
    sell = {}
    for row in orders:
        if row[2]  == 'buy' :
            buy[row] = row.get(row)
        elif row[2] == 'sell':
            sell[row] = row.get(row)
    for key,val in buy.items():
        for key1,val1 in sell.items():
            if key <= key1:
                output += val
                buy[key].append
            elif key > key1:
                output -=val
    return output