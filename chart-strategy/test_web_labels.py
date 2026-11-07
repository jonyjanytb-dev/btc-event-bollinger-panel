import unittest
from shadow.web import chart_labels


class ChartLabelsTests(unittest.TestCase):
    def test_dual_band_touch_labels(self):
        self.assertEqual(chart_labels('HIGHER','BOTH_LOWER_TOUCHED'),('看涨','5m 与 1m 已收盘 K 线均触及布林下轨'))
        self.assertEqual(chart_labels('LOWER','BOTH_UPPER_TOUCHED'),('看跌','5m 与 1m 已收盘 K 线均触及布林上轨'))
        self.assertEqual(chart_labels('NO_SIGNAL','AMBIGUOUS_TOUCH')[1],'上下轨同时触及，方向不明确')


if __name__=='__main__':unittest.main()
