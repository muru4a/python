"""
Author :  Murugesan Alagusundaram

Python version : python 3.7

Usage of command line arguments: python3 /usr/share/dict/words cat dog

Solution : Read the file and build the graph with dictionary of words as vertex
           Breadth first search (BFS) algorithm to traverse the vertex and search the path
"""


from collections import defaultdict,deque
from itertools import product
import os,sys


class Transform:
    def __init__(self, filename, start,end):
        self.filename = filename
        self.start = start
        self.end = end

    # Read file and get the words
    def get_words(self):
        file = os.path.join(os.path.dirname(__file__), self.filename)
        for line in open(file, 'r'):
            yield line[:-1]

    # Build the graph with dictionary of words as vertex
    def build_graph(self,words):
        buckets = defaultdict(list)
        graph = defaultdict(set)

        for word in words:
            for i in range(len(word)):
                bucket = '{}_{}'.format(word[:i], word[i + 1:])
                buckets[bucket].append(word)

            # add vertices and edges for words in the same bucket
        for bucket, mutual_neighbors in buckets.items():
            for word1, word2 in product(mutual_neighbors, repeat=2):
                if word1 != word2:
                    graph[word1].add(word2)
                    graph[word2].add(word1)

        return graph

    # Breadth first search (BFS) algorithm to traverse and search the path
    def traverse(self,graph, starting_vertex):
        visited = set()
        queue = deque([[starting_vertex]])
        while queue:
            path = queue.popleft()
            vertex = path[-1]
            yield vertex, path
            for neighbor in graph[vertex] - visited:
                visited.add(neighbor)
                queue.append(path + [neighbor])


def main():

    # check for length of start and end
    if len(str(sys.argv[2])) != len(str(sys.argv[3])):
        raise TypeError("Length of start and end word should be same")

    transform = Transform(str(sys.argv[1]), str(sys.argv[2]).lower(), str(sys.argv[3]).lower())

    for vertex, path in transform.traverse(transform.build_graph(transform.get_words()), transform.start):
        if vertex == transform.end:
            print(' > '.join(path))


if __name__ == "__main__":
    main()
