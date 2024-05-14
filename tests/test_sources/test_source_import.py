from pathlib import Path
import importlib

from indra_cogex.sources.cli import _iter_processors


def get_processor_classes():
    # Assumes that all processors are in their own submodules of indra_cogex.sources
    # either in directories or in python files
    from indra_cogex.sources import Processor
    from indra_cogex import sources

    sources_path = Path(sources.__file__).parent
    yielded = set()
    for submodule_path in sources_path.iterdir():
        if submodule_path.name.startswith("_"):
            continue
        submodule_name = submodule_path.name
        if submodule_name.endswith(".py"):
            submodule_name = submodule_name[:-3]
        submodule_path = f"indra_cogex.sources.{submodule_name}"
        submodule = importlib.import_module(submodule_path)
        for name in dir(submodule):
            if name.startswith("_"):
                continue
            obj = getattr(submodule, name)
            if isinstance(obj, type) and issubclass(obj, Processor):
                yielded.add(obj)
                yield obj


def test_source_import():
    all_subclasses = {cls.__name__ for cls in get_processor_classes()}
    iter_resolve_classes = {cls.__name__ for cls in _iter_processors()}
    ignore = {"Processor", "PyoboProcessor", "WikiDataProcessor"}
    actual = iter_resolve_classes - ignore
    expected = all_subclasses - ignore
    assert expected == actual, (
        f"Expected {expected} but got {actual}.\nMissing: "
        f"{expected - actual},\nExtra: {actual - expected}"
    )
