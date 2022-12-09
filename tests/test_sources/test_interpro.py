import unittest
from pathlib import Path

from indra_cogex.sources.interpro import get_interpro_to_goa, process_go_mapping_line

HERE = Path(__file__).parent.resolve()
RESOURCES = HERE.parent.joinpath("resources").resolve()


class TestInterpro(unittest.TestCase):
    """Test case for InterPro."""

    def test_go_mapping_line(self):
        line = "InterPro:IPR000003 Retinoid X receptor/HNF4 > GO:DNA binding ; GO:0003677"
        self.assertEqual(("IPR000003", "GO:0003677"), process_go_mapping_line(line))

    def test_go_mapping(self):
        path = RESOURCES.joinpath("test_interpro_go.txt")
        data = get_interpro_to_goa(path=path)
        self.assertEqual({"IPR000003": {"GO:0003677", "GO:0003707"}}, data)
