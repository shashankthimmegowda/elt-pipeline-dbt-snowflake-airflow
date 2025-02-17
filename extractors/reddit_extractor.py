"""Reddit data extractor using PRAW and async httpx."""

import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx
import praw
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import get_settings

logger = structlog.get_logger(__name__)

# Target subreddits for extraction
DEFAULT_SUBREDDITS = [
    "dataengineering",
    "datascience",
    "python",
    "machinelearning",
    "analytics",
]

# Reddit API rate limit: 100 requests per minute
RATE_LIMIT_DELAY = 0.6  # seconds between requests


class RedditExtractor:
    """Extracts posts and comments from Reddit using PRAW."""

    def __init__(self):
        settings = get_settings()
        self.reddit = praw.Reddit(
            client_id=settings.reddit.client_id,
            client_secret=settings.reddit.client_secret,
            user_agent=settings.reddit.user_agent,
        )
        self.extracted_at = datetime.now(timezone.utc).isoformat()

    def extract_subreddit_posts(
        self,
        subreddit_name: str,
        sort: str = "hot",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Extract posts from a subreddit.

        Args:
            subreddit_name: Name of the subreddit (without r/)
            sort: Sort method - hot, new, top, rising
            limit: Maximum number of posts to extract

        Returns:
            List of post dictionaries ready for loading
        """
        logger.info(
            "extracting_subreddit_posts",
            subreddit=subreddit_name,
            sort=sort,
            limit=limit,
        )

        subreddit = self.reddit.subreddit(subreddit_name)
        sort_methods = {
            "hot": subreddit.hot,
            "new": subreddit.new,
            "top": subreddit.top,
            "rising": subreddit.rising,
        }

        fetch_method = sort_methods.get(sort, subreddit.hot)
        posts = []

        for submission in fetch_method(limit=limit):
            post = {
                "post_id": submission.id,
                "subreddit": subreddit_name,
                "title": submission.title,
                "selftext": submission.selftext[:10000] if submission.selftext else None,
                "author": str(submission.author) if submission.author else "[deleted]",
                "score": submission.score,
                "upvote_ratio": submission.upvote_ratio,
                "num_comments": submission.num_comments,
                "created_utc": datetime.fromtimestamp(
                    submission.created_utc, tz=timezone.utc
                ).isoformat(),
                "url": submission.url,
                "permalink": f"https://reddit.com{submission.permalink}",
                "is_self": submission.is_self,
                "link_flair_text": submission.link_flair_text,
                "over_18": submission.over_18,
                "spoiler": submission.spoiler,
                "stickied": submission.stickied,
                "total_awards_received": submission.total_awards_received,
                "extracted_at": self.extracted_at,
                "sort_method": sort,
            }
            posts.append(post)

        logger.info(
            "extracted_subreddit_posts",
            subreddit=subreddit_name,
            count=len(posts),
        )
        return posts

    def extract_post_comments(
        self,
        post_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Extract top-level comments for a post.

        Args:
            post_id: Reddit post ID
            limit: Maximum comments to extract

        Returns:
            List of comment dictionaries
        """
        submission = self.reddit.submission(id=post_id)
        submission.comments.replace_more(limit=0)

        comments = []
        for comment in submission.comments[:limit]:
            comments.append(
                {
                    "comment_id": comment.id,
                    "post_id": post_id,
                    "author": str(comment.author) if comment.author else "[deleted]",
                    "body": comment.body[:10000] if comment.body else None,
                    "score": comment.score,
                    "created_utc": datetime.fromtimestamp(
                        comment.created_utc, tz=timezone.utc
                    ).isoformat(),
                    "is_submitter": comment.is_submitter,
                    "parent_id": comment.parent_id,
                    "extracted_at": self.extracted_at,
                }
            )

        return comments

    def extract_all(
        self,
        subreddits: list[str] | None = None,
        posts_per_sub: int = 100,
        comments_per_post: int = 20,
    ) -> dict[str, list[dict[str, Any]]]:
        """Full extraction of posts and comments from multiple subreddits.

        Returns:
            Dictionary with 'posts' and 'comments' keys
        """
        subreddits = subreddits or DEFAULT_SUBREDDITS
        all_posts = []
        all_comments = []

        for sub in subreddits:
            posts = self.extract_subreddit_posts(sub, limit=posts_per_sub)
            all_posts.extend(posts)

            # Extract comments for top posts (by score)
            top_posts = sorted(posts, key=lambda p: p["score"], reverse=True)[:10]
            for post in top_posts:
                comments = self.extract_post_comments(
                    post["post_id"], limit=comments_per_post
                )
                all_comments.extend(comments)

        logger.info(
            "extraction_complete",
            total_posts=len(all_posts),
            total_comments=len(all_comments),
        )

        return {"posts": all_posts, "comments": all_comments}


class AsyncRedditExtractor:
    """Async Reddit extractor using httpx for higher throughput."""

    BASE_URL = "https://oauth.reddit.com"

    def __init__(self):
        settings = get_settings()
        self._client_id = settings.reddit.client_id
        self._client_secret = settings.reddit.client_secret
        self._user_agent = settings.reddit.user_agent
        self._token: str | None = None
        self.extracted_at = datetime.now(timezone.utc).isoformat()

    async def _authenticate(self, client: httpx.AsyncClient) -> None:
        """Obtain OAuth2 bearer token."""
        auth = httpx.BasicAuth(self._client_id, self._client_secret)
        resp = await client.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=auth,
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": self._user_agent},
        )
        resp.raise_for_status()
        self._token = resp.json()["access_token"]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def _get(self, client: httpx.AsyncClient, endpoint: str, params: dict | None = None) -> dict:
        """Make authenticated GET request with retry."""
        if not self._token:
            await self._authenticate(client)

        resp = await client.get(
            f"{self.BASE_URL}{endpoint}",
            params=params,
            headers={
                "Authorization": f"Bearer {self._token}",
                "User-Agent": self._user_agent,
            },
        )

        # Handle rate limiting
        remaining = int(resp.headers.get("x-ratelimit-remaining", 100))
        if remaining < 5:
            reset_seconds = float(resp.headers.get("x-ratelimit-reset", 60))
            logger.warning("rate_limit_approaching", remaining=remaining, reset=reset_seconds)
            await asyncio.sleep(min(reset_seconds, 60))

        resp.raise_for_status()
        return resp.json()

    async def extract_subreddit_posts(
        self,
        client: httpx.AsyncClient,
        subreddit: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Extract posts from a subreddit asynchronously."""
        posts = []
        after = None

        while len(posts) < limit:
            batch_size = min(100, limit - len(posts))
            params = {"limit": batch_size}
            if after:
                params["after"] = after

            data = await self._get(client, f"/r/{subreddit}/hot", params)
            children = data.get("data", {}).get("children", [])
            if not children:
                break

            for child in children:
                post_data = child["data"]
                posts.append(
                    {
                        "post_id": post_data["id"],
                        "subreddit": subreddit,
                        "title": post_data["title"],
                        "selftext": (post_data.get("selftext") or "")[:10000],
                        "author": post_data.get("author", "[deleted]"),
                        "score": post_data["score"],
                        "upvote_ratio": post_data.get("upvote_ratio", 0),
                        "num_comments": post_data["num_comments"],
                        "created_utc": datetime.fromtimestamp(
                            post_data["created_utc"], tz=timezone.utc
                        ).isoformat(),
                        "url": post_data["url"],
                        "permalink": f"https://reddit.com{post_data['permalink']}",
                        "is_self": post_data["is_self"],
                        "link_flair_text": post_data.get("link_flair_text"),
                        "over_18": post_data.get("over_18", False),
                        "extracted_at": self.extracted_at,
                    }
                )

            after = data["data"].get("after")
            if not after:
                break
            await asyncio.sleep(RATE_LIMIT_DELAY)

        return posts

    async def extract_all(
        self,
        subreddits: list[str] | None = None,
        posts_per_sub: int = 100,
    ) -> list[dict[str, Any]]:
        """Extract from multiple subreddits concurrently."""
        subreddits = subreddits or DEFAULT_SUBREDDITS
        async with httpx.AsyncClient(timeout=30) as client:
            await self._authenticate(client)
            tasks = [
                self.extract_subreddit_posts(client, sub, posts_per_sub)
                for sub in subreddits
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        all_posts = []
        for result in results:
            if isinstance(result, Exception):
                logger.error("extraction_failed", error=str(result))
                continue
            all_posts.extend(result)

        return all_posts
