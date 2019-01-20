import unittest
import sys
import os
from pyteomics import mzid

sys.path.insert(0, os.path.abspath('..'))
import  utils.mzident_reader as mgf_search_result_annotator


class MzIdParserTest(unittest.TestCase):
    """
    Test mzid parser functions
    """
    def setUp(self):
        self.testfile = os.path.join(os.path.dirname(__file__), "testfiles", "test.mzid")
        # print(dir(mgf_search_result_annotator))

    def testGet(self):
        pass
        # score_field = "Scaffold:Peptide Probability"
        # psms =mgf_search_result_annotator.parser_mzident2(filename=self.testfile, score_field=score_field)
        # print(psms[:5])
        # self.assertEqual("", psms)

    def testGetPeakFile(self):
        # pass
        score_field = "Scaffold:Peptide Probability"
        score_field, peak_file = mgf_search_result_annotator.get_scfield_peakfile(filename=self.testfile)
        print(score_field, peak_file)

if __name__ == "__main__":
    unittest.main()
