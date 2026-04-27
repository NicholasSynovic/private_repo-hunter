from ghapi.all import GhApi, paged


class GitHub:
    def __init__(self, owner: str, repo: str) -> None:
        """
        Initialize with repository details.
        Assumes GITHUB_TOKEN is set in the environment.
        """
        self.owner = owner
        self.repo = repo
        self.gh = GhApi(owner=owner, repo=repo)

    def get_all_issues(self) -> list[dict]:
        """
        Retrieves all issues (all states) using serial pagination.
        Returns a list of standard Python dictionaries.
        """
        # Create the generator for all pages
        pages = paged(
            self.gh.issues.list_for_repo,
            state="all",
            sort="created",
            direction="asc",
            per_page=100,
        )

        # Flatten pages and convert AttrDicts to standard dicts
        # Fastcore's L.map(dict) is the cleanest way to cast the objects
        data = []
        for page in pages:
            data.extend(page.map(dict))

        return data
