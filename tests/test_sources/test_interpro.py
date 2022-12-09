import unittest
from pathlib import Path

from indra_cogex.sources.interpro import (
    _help_parent_to_children,
    get_interpro_to_goa,
    process_go_mapping_line,
)

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

    def test_parse_tree(self):
        path = RESOURCES.joinpath("test_interpro_tree.txt")
        with path.open() as file:
            parent_to_children = _help_parent_to_children(file)

        # Root term
        self.assertIn("IPR000053", set(parent_to_children))
        self.assertEqual(
            {"IPR013466", "IPR018090"},
            parent_to_children["IPR000053"],
        )

        # Middle term
        self.assertIn("IPR013466", set(parent_to_children))
        self.assertEqual(
            {"IPR017713", "IPR028579"},
            parent_to_children["IPR013466"],
        )

        # Leaves shouldn't be included
        self.assertNotIn("IPR028579", set(parent_to_children))
        self.assertNotIn("IPR017713", set(parent_to_children))
        self.assertNotIn("IPR013465", set(parent_to_children))
