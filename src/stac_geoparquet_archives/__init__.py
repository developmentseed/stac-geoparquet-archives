from rich.console import Console


def main(console: Console | None = None) -> None:
    """Run the `stac-geoparquet-archives` command line interface.

    Args:
        console: The `Console` to print to. Defaults to a new `Console`.
    """
    console = console or Console()
    console.print("Hello from [bold]stac-geoparquet-archives[/bold]!")
