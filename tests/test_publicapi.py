import inspect


def test_wildcard_import():
    bbsa = __import__('bonobo_sqlalchemy')
    assert bbsa.__version__

    for name in dir(bbsa):
        # ignore attributes starting by underscores
        if name.startswith('_'):
            continue
        attr = getattr(bbsa, name)
        if inspect.ismodule(attr):
            continue

        assert name in bbsa.__all__
