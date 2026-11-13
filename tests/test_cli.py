from rich.console import Console

import stac_geoparquet_archives


def test_main() -> None:
    console = Console()
    with console.capture() as capture:
        stac_geoparquet_archives.main(console)
    assert capture.get() == "Hello from stac-geoparquet-archives!\n"
