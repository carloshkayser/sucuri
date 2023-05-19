from pyDF import *

words = """
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed non risus. Suspendisse lectus tortor, dignissim sit amet, adipiscing nec, ultricies sed, dolor. Cras elementum ultrices diam. Maecenas ligula massa, varius a, semper congue, euismod non, mi. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed non risus. Suspendisse lectus tortor, dignissim sit amet, adipiscing nec, ultricies sed, dolor. Cras elementum ultrices diam. Maecenas ligula massa, varius a, semper congue, euismod non, mi. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed non risus. Suspendisse lectus tortor, dignissim sit amet, adipiscing nec, ultricies sed, dolor. Cras elementum ultrices diam. Maecenas ligula massa, varius a, semper congue, euismod non, mi.
"""

def split_words(args):
    return args[0].split()

def count_words(args):
    counts = {}
    for word in args[0]:
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1
    return counts

def sum_counts(args):
    total = {}
    for count in args:
        for word in count:
            if word in total:
                total[word] += count[word]
            else:
                total[word] = count[word]
    return total

graph = DFGraph()
sched = Scheduler(graph, n_workers=1, mpi_enabled=False)

A = Feeder(words)
graph.add(A)
B = Node(split_words, 1)
graph.add(B)
C = Node(count_words, 1)
graph.add(C)
D = Node(sum_counts, 1)
graph.add(D)

A.add_edge(B, 0)
B.add_edge(C, 0)
C.add_edge(D, 0)

sched.start()
