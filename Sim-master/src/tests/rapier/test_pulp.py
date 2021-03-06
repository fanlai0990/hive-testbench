import unittest
from pulp import *


class TestPuLP (unittest.TestCase):
    def test_example(self):
        # declare your variables
        x1 = LpVariable("x1", 0, 40)  # 0<= x1 <= 40
        x2 = LpVariable("x2", 0, 1000)  # 0<= x2 <= 1000

        # defines the problem
        prob = LpProblem("problem", LpMaximize)

        # defines the constraints
        prob += 2 * x1 + x2 <= 100
        prob += x1 + x2 <= 80
        prob += x1 <= 40
        prob += x1 >= 0
        prob += x2 >= 0

        # defines the objective function to maximize
        prob += 3 * x1 + 2 * x2

        # solve the problem
        status = prob.solve()
        print LpStatus[status]

        # print the results x1 = 20, x2 = 60
        self.assertEqual(value(x1), 20)
        self.assertEqual(value(x2), 60)

if __name__ == '__main__':
    unittest.main()
