"""
ForML config unit tests.
"""
# pylint: disable=protected-access,no-self-use


class TestConf:
    """Conf unit tests.
    """
    def test_registry(self, conf):
        """Test the registry config field.
        """
        provider = conf.REGISTRY
        assert provider.name == 'filesystem'

    def test_engine(self, conf):
        """Test the engine config field.
        """
        provider = conf.ENGINE
        assert provider.name == 'devio'

    def test_runner(self, conf):
        """Test the runner config field.
        """
        provider = conf.RUNNER
        assert provider.name == 'dask'
