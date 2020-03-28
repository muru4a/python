import collections,heapq
def averge_score(items):
    result = []
    d=collections.defaultdict(list)
    for student,mark in items:
        d[student].append(mark)
    for student,marks in d.items():
        avg= sum(heapq.nlargest(5,marks))//5
        result.append([student,avg])
    return result

print(averge_score([[1,91],[1,92],[2,93],[2,97],[1,60],[2,77],[1,65],[1,87],[1,100],[2,100],[2,76]]))