import unittest

from shadow.ladder import LadderError, next_step, validate_stakes


class LadderTests(unittest.TestCase):
    def test_custom_stakes_and_limits(self):
        self.assertEqual(validate_stakes(["5", "7", "15"], "200"), ("5", "7", "15"))
        self.assertEqual(validate_stakes(["6.25", "9", "18.5"], "200"),
                         ("6.25", "9", "18.5"))
        for values in (["4", "7", "15"], ["5", "201", "15"],
                       ["5", "NaN", "15"], ["5", "7.001", "15"],
                       ["5", "7"], [5, 7, 15]):
            with self.assertRaises(LadderError):
                validate_stakes(values, "200")

    def test_loss_across_directions_advances_and_third_loss_resets(self):
        self.assertEqual(next_step(0, "LOSS"), 1)
        self.assertEqual(next_step(1, "LOSS"), 2)
        self.assertEqual(next_step(2, "LOSS"), 0)

    def test_win_and_equal_stake_refund_reset(self):
        for step in range(3):
            self.assertEqual(next_step(step, "WIN"), 0)
            self.assertEqual(next_step(step, "EQUAL"), 0)

    def test_unverified_result_cannot_advance(self):
        for result in ("PENDING", "MISSING", "API_ACCEPTED_UNVERIFIED", None):
            with self.assertRaises(LadderError):
                next_step(1, result)


if __name__ == "__main__":
    unittest.main()
