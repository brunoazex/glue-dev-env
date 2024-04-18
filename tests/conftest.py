def pytest_addoption(parser):
    parser.addoption("--JOB_NAME", action="store", help="Glue`s Job Name")


def pytest_generate_tests(metafunc):
    JOB_NAME = metafunc.config.option.JOB_NAME
    if "JOB_NAME" in metafunc.fixturenames and JOB_NAME is not None:
        metafunc.parametrize("JOB_NAME", [JOB_NAME])
