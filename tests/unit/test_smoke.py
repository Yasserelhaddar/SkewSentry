def test_version_import():
    import skewsentry

    assert isinstance(skewsentry.__version__, str)
    assert len(skewsentry.__version__) > 0

