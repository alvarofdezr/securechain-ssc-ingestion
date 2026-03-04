from re import compile, match, sub
from urllib.parse import urlparse


class RepoNormalizer:
    def __init__(self):
        self.normalized_url: str | None = None
        self.repo_hosts: set[str] = {
            "github.com",
            "gitlab.com",
            "bitbucket.org",
            "www.github.com",
            "www.gitlab.com",
            "www.bitbucket.org",
        }

    def normalize(self, raw_url: str | None) -> str | None:
        if not raw_url:
            return None
        u = self.normalize_git(raw_url)
        parsed = urlparse(u)
        if not parsed.scheme:
            u = "https://" + u
            parsed = urlparse(u)
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        clean = sub(r"\.git/?$", "", clean).rstrip("/")
        git_hosting_pattern = r"(https?://[^/]+/[^/]+/[^/]+)(?:/.*)?$"
        git_match = match(git_hosting_pattern, clean)
        if git_match:
            host = parsed.netloc.lower()
            if any(
                platform in host
                for platform in ["github.com", "gitlab.com", "bitbucket.org"]
            ):
                clean = git_match.group(1)
        self.normalized_url = clean
        return clean

    def normalize_git(self, raw_url: str) -> str:
        u = raw_url.strip()
        if u.startswith("git+"):
            u = u[4:]
        m = match(r"git@([^:]+):(.+)", u)
        if m:
            host, path = m.groups()
            u = f"https://{host}/{path}"
        if u.startswith("ssh://git@"):
            u = "https://" + u[len("ssh://git@") :]
        if u.startswith("git://"):
            u = "https://" + u[len("git://") :]
        return u

    def check(self) -> bool:
        if not self.normalized_url:
            return False
        parsed = urlparse(self.normalized_url.strip())
        host = parsed.netloc.lower()
        if host not in self.repo_hosts:
            return False
        path = parsed.path.strip("/")
        if not path:
            return False
        parts = path.split("/")
        if parts[0].lower() == "orgs":
            return False
        if len(parts) < 2:
            return False
        owner, repo = parts[0], parts[1]
        if not owner or not repo:
            return False
        allowed = compile(r"^[A-Za-z0-9._-]+$")
        if not allowed.match(owner) or not allowed.match(repo):
            return False
        return True
