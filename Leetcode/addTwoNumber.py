"""
# 2: Add Two Numbers 
You are given two non-empty linked lists representing two non-negative integers. The digits are stored in reverse order and each of their nodes contain a single digit. Add the two numbers and return it as a linked list.

You may assume the two numbers do not contain any leading zero, except the number 0 itself.

Example:

Input: (2 -> 4 -> 3) + (5 -> 6 -> 4)
Output: 7 -> 0 -> 8
Explanation: 342 + 465 = 807.

Solution:
1. create newlist with dummy node  newlist = Listnode(0)
2. point nodelist to temp temp = newlist
3. append the temp node with total sum 
4. Intialise the total sum =0, carry =0
5. Iterate the linked list L1 and L2 and carry until is empty 
6. if l1: totalsum +=l1.data and move the pointer to next node 
7. if l2: totalsum +=l2.data and move the pointer to next node
8. Add the carry to total sum : totalsum += carry
9. carry = totalsum // 10
10. temp.next = ListNode(totalsum % 10)
11. reassign totalsum=0
12. temp = temp.next
13. return newlist.next

tc : O(max(m,n))
sc: O(max(m,n))
"""

# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, x):
#         self.val = x
#         self.next = None

class Solution:
    def addTwoNumbers(self, l1: ListNode, l2: ListNode) -> ListNode:
        newlist = ListNode(0)
        temp = newlist
        totalsum = 0
        carry = 0
        while l1 or l2 or carry:
            if l1:
                totalsum +=l1.val
                l1 = l1.next
            if l2:
                totalsum +=l2.val
                l2 = l2.next

            totalsum +=carry
            carry = totalsum // 10
            temp.next = ListNode(totalsum % 10)
            totalsum = 0
            temp = temp.next
        return newlist.next




