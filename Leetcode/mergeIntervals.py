"""
Given a collection of intervals, merge all overlapping intervals.

Example 1:

Input: [[1,3],[2,6],[8,10],[15,18]]
Output: [[1,6],[8,10],[15,18]]
Explanation: Since intervals [1,3] and [2,6] overlaps, merge them into [1,6].
Example 2:

Input: [[1,4],[4,5]]
Output: [[1,5]]
Explanation: Intervals [1,4] and [4,5] are considered overlapping.

Solution:

1. sort the intervals [[1,3],[2,6],[8,10],[15,18]]
2. if seocond_start <= first_end then its overlap
3. find_start = min(fisrt_start,second_start)
4. find_end  = max(first_end,second_end)
5. remove the first element and update second

"""

def mergeIntervals(intervals):

    intervals.sort(key=lambda x:x[0])
    print(intervals)
    i=1
    while i < len(intervals):
        if intervals[i][0] <= intervals[i-1][1]:
            intervals[i-1][0] = min(intervals[i-1][0],intervals[i][0])
            intervals[i-1][1] = max(intervals[i-1][1],intervals[i][1])

            intervals.pop(i)
        else:
            i=i+1
    return intervals

print(mergeIntervals([[1,3],[2,6],[8,10],[15,18]]))


