from pkg_resources import WorkingSet


def reverseOrder(s):
    words_list = s.split()
    return '"%s"'%' '.join(words_list[::-1])


print(reverseOrder("the sky is blue"))
