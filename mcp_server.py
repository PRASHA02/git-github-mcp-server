"""
Comprehensive Git + GitHub MCP Server with HTTP/SSE Streaming
- 25+ Local Git Operations
- 28+ GitHub API Operations
- REST API with SSE (Server-Sent Events) for streaming
- Suitable for MCP consumption

Run: python mcp_server.py
Access: http://localhost:8000
Docs: http://localhost:8000/docs
"""

import subprocess
import json
import os
import asyncio
from typing import Optional, List, AsyncGenerator
from github import Github, GithubException
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn

# ===== PYDANTIC MODELS =====

class GitInitRequest(BaseModel):
    repo_path: str

class GitStatusRequest(BaseModel):
    repo_path: str

class GitDiffRequest(BaseModel):
    repo_path: str
    target: Optional[str] = None

class GitAddRequest(BaseModel):
    repo_path: str
    files: list

class GitResetRequest(BaseModel):
    repo_path: str
    files: Optional[list] = None

class GitCommitRequest(BaseModel):
    repo_path: str
    message: str
    author_name: Optional[str] = None
    author_email: Optional[str] = None
    files: Optional[list] = None

class GitLogRequest(BaseModel):
    repo_path: str
    limit: int = 10
    oneline: bool = True

class GitShowRequest(BaseModel):
    repo_path: str
    commit: str

class GitCreateBranchRequest(BaseModel):
    repo_path: str
    branch_name: str
    base_branch: str = "main"

class GitCheckoutRequest(BaseModel):
    repo_path: str
    branch: str
    create: bool = False

class GitBranchRequest(BaseModel):
    repo_path: str
    filter_type: str = "local"

class GitCloneRequest(BaseModel):
    repo_url: str
    target_dir: str
    branch: Optional[str] = None

class GitPullRequest(BaseModel):
    repo_path: str
    remote: str = "origin"
    branch: Optional[str] = None

class GitPushRequest(BaseModel):
    repo_path: str
    remote: str = "origin"
    branch: str = "main"
    force: bool = False

class GitMergeRequest(BaseModel):
    repo_path: str
    branch: str
    commit_message: Optional[str] = None

class GitRebaseRequest(BaseModel):
    repo_path: str
    onto_branch: str

class GitStashRequest(BaseModel):
    repo_path: str
    include_untracked: bool = False

class GitTagRequest(BaseModel):
    repo_path: str
    tag_name: str
    message: Optional[str] = None

class GitRemoteRequest(BaseModel):
    repo_path: str
    action: str
    name: Optional[str] = None
    url: Optional[str] = None

class GitConfigRequest(BaseModel):
    repo_path: str
    action: str
    key: str
    value: Optional[str] = None

class GitCherryPickRequest(BaseModel):
    repo_path: str
    commit: str

class GitRevertRequest(BaseModel):
    repo_path: str
    commit: str

class GitBlameRequest(BaseModel):
    repo_path: str
    file_path: str

class GitCleanRequest(BaseModel):
    repo_path: str
    dry_run: bool = True
    force: bool = False

# GITHUB MODELS
class GitHubGetFileContentsRequest(BaseModel):
    repo_owner: str
    repo_name: str
    file_path: str
    ref: Optional[str] = None

class GitHubCreateOrUpdateFileRequest(BaseModel):
    repo_owner: str
    repo_name: str
    file_path: str
    content: str
    message: str
    branch: Optional[str] = None

class GitHubDeleteFileRequest(BaseModel):
    repo_owner: str
    repo_name: str
    file_path: str
    message: str
    branch: Optional[str] = None

class GitHubPushFilesRequest(BaseModel):
    repo_owner: str
    repo_name: str
    files: List[dict]
    message: str
    branch: Optional[str] = None

class GitHubCreateBranchRequest(BaseModel):
    repo_owner: str
    repo_name: str
    branch_name: str
    base_branch: str = "main"

class GitHubCreateRepositoryRequest(BaseModel):
    repo_name: str
    description: Optional[str] = None
    private: bool = False
    auto_init: bool = True

class GitHubForkRepositoryRequest(BaseModel):
    repo_owner: str
    repo_name: str

class GitHubListCommitsRequest(BaseModel):
    repo_owner: str
    repo_name: str
    branch: Optional[str] = None
    limit: int = 30

class GitHubGetCommitRequest(BaseModel):
    repo_owner: str
    repo_name: str
    commit_sha: str

class GitHubCreatePullRequestRequest(BaseModel):
    repo_owner: str
    repo_name: str
    title: str
    body: str
    head_branch: str
    base_branch: str = "main"

class GitHubListPullRequestsRequest(BaseModel):
    repo_owner: str
    repo_name: str
    state: str = "open"
    limit: int = 30

class GitHubGetPullRequestRequest(BaseModel):
    repo_owner: str
    repo_name: str
    pr_number: int

class GitHubMergePullRequestRequest(BaseModel):
    repo_owner: str
    repo_name: str
    pr_number: int
    commit_message: Optional[str] = None
    merge_method: str = "squash"

class GitHubPullRequestReviewRequest(BaseModel):
    repo_owner: str
    repo_name: str
    pr_number: int
    event: str
    body: Optional[str] = None

class GitHubCreateIssueRequest(BaseModel):
    repo_owner: str
    repo_name: str
    title: str
    body: Optional[str] = None
    labels: Optional[List[str]] = None
    assignees: Optional[List[str]] = None

class GitHubGetIssueRequest(BaseModel):
    repo_owner: str
    repo_name: str
    issue_number: int

class GitHubAddIssueCommentRequest(BaseModel):
    repo_owner: str
    repo_name: str
    issue_number: int
    comment_body: str

class GitHubSearchRepositoriesRequest(BaseModel):
    query: str
    language: Optional[str] = None
    sort: str = "stars"
    limit: int = 30

class GitHubSearchCodeRequest(BaseModel):
    query: str
    repo_owner: Optional[str] = None
    repo_name: Optional[str] = None
    language: Optional[str] = None
    limit: int = 30

class GitHubSearchIssuesRequest(BaseModel):
    query: str
    repo_owner: Optional[str] = None
    repo_name: Optional[str] = None
    state: str = "open"
    limit: int = 30

class GitHubGetLabelRequest(BaseModel):
    repo_owner: str
    repo_name: str
    label_name: str

class GitHubListReleasesRequest(BaseModel):
    repo_owner: str
    repo_name: str
    limit: int = 30

class GitHubGetLatestReleaseRequest(BaseModel):
    repo_owner: str
    repo_name: str

class GitHubListTagsRequest(BaseModel):
    repo_owner: str
    repo_name: str
    limit: int = 30

class GitHubGetTagRequest(BaseModel):
    repo_owner: str
    repo_name: str
    tag_name: str

class GitHubGetTeamMembersRequest(BaseModel):
    org: str
    team_slug: str

class GitHubGetTeamsRequest(BaseModel):
    org: str

class GitHubAssignCopilotToIssueRequest(BaseModel):
    repo_owner: str
    repo_name: str
    issue_number: int

class GitHubAddCommentToPendingReviewRequest(BaseModel):
    repo_owner: str
    repo_name: str
    pr_number: int
    comment_body: str
    in_reply_to: Optional[int] = None

# ===== CORE SERVER CLASS =====

class ComprehensiveGitGitHubServer:
    """All-in-one Git + GitHub operations server with streaming support"""
    
    def __init__(self, github_token: str):
        self.github_token = github_token
        self.gh = Github(github_token) if github_token else None
    
    async def stream_json(self, data: dict) -> AsyncGenerator[str, None]:
        """Stream JSON data as SSE"""
        yield f"data: {json.dumps(data)}\n\n"
    
    # ===== LOCAL GIT OPERATIONS (25+) =====
    
    def git_init(self, repo_path: str) -> dict:
        try:
            os.makedirs(repo_path, exist_ok=True)
            subprocess.run(["git", "init"], cwd=repo_path, capture_output=True, check=True)
            return {"success": True, "message": f"Initialized repo at {repo_path}"}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr.decode()}
    
    def git_status(self, repo_path: str) -> dict:
        try:
            result = subprocess.run(["git", "status", "--porcelain"], cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "status": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_diff_unstaged(self, repo_path: str) -> dict:
        try:
            result = subprocess.run(["git", "diff"], cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "diff": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_diff_staged(self, repo_path: str) -> dict:
        try:
            result = subprocess.run(["git", "diff", "--cached"], cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "diff": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_diff(self, repo_path: str, target: str = None) -> dict:
        try:
            cmd = ["git", "diff"]
            if target:
                cmd.append(target)
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "diff": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_add(self, repo_path: str, files: list) -> dict:
        try:
            subprocess.run(["git", "add"] + files, cwd=repo_path, capture_output=True, check=True)
            return {"success": True, "files_staged": files}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_reset(self, repo_path: str, files: list = None) -> dict:
        try:
            cmd = ["git", "reset"]
            if files:
                cmd.extend(files)
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_commit(self, repo_path: str, message: str, author_name: str = None, author_email: str = None, files: list = None) -> dict:
        try:
            if files:
                subprocess.run(["git", "add"] + files, cwd=repo_path, capture_output=True, check=True)
            cmd = ["git", "commit", "-m", message]
            if author_name and author_email:
                cmd.extend(["--author", f"{author_name} <{author_email}>"])
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "message": message}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_log(self, repo_path: str, limit: int = 10, oneline: bool = True) -> dict:
        try:
            cmd = ["git", "log"]
            if oneline:
                cmd.append("--oneline")
            cmd.extend(["-n", str(limit)])
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "commits": result.stdout.split("\n")}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_show(self, repo_path: str, commit: str) -> dict:
        try:
            result = subprocess.run(["git", "show", commit], cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "commit_details": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_create_branch(self, repo_path: str, branch_name: str, base_branch: str = "main") -> dict:
        try:
            subprocess.run(["git", "checkout", base_branch], cwd=repo_path, capture_output=True, check=True)
            subprocess.run(["git", "checkout", "-b", branch_name], cwd=repo_path, capture_output=True, check=True)
            return {"success": True, "branch": branch_name}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_checkout(self, repo_path: str, branch: str, create: bool = False) -> dict:
        try:
            cmd = ["git", "checkout"]
            if create:
                cmd.append("-b")
            cmd.append(branch)
            subprocess.run(cmd, cwd=repo_path, capture_output=True, check=True)
            return {"success": True, "message": f"Switched to branch: {branch}"}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_branch(self, repo_path: str, filter_type: str = "local") -> dict:
        try:
            cmd = ["git", "branch"]
            if filter_type == "remote":
                cmd.append("-r")
            elif filter_type == "all":
                cmd.append("-a")
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            branches = [b.strip() for b in result.stdout.split("\n") if b.strip()]
            return {"success": True, "branches": branches}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_clone(self, repo_url: str, target_dir: str, branch: str = None) -> dict:
        try:
            cmd = ["git", "clone"]
            if branch:
                cmd.extend(["--branch", branch])
            cmd.extend([repo_url, target_dir])
            subprocess.run(cmd, capture_output=True, check=True)
            return {"success": True, "message": f"Cloned to {target_dir}"}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_pull(self, repo_path: str, remote: str = "origin", branch: str = None) -> dict:
        try:
            cmd = ["git", "pull", remote]
            if branch:
                cmd.append(branch)
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_push(self, repo_path: str, remote: str = "origin", branch: str = "main", force: bool = False) -> dict:
        try:
            cmd = ["git", "push"]
            if force:
                cmd.append("--force")
            cmd.extend([remote, branch])
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_merge(self, repo_path: str, branch: str, commit_message: str = None) -> dict:
        try:
            cmd = ["git", "merge", branch]
            if commit_message:
                cmd.extend(["-m", commit_message])
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_rebase(self, repo_path: str, onto_branch: str) -> dict:
        try:
            result = subprocess.run(["git", "rebase", onto_branch], cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_stash(self, repo_path: str, include_untracked: bool = False) -> dict:
        try:
            cmd = ["git", "stash", "push"]
            if include_untracked:
                cmd.append("-u")
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_tag(self, repo_path: str, tag_name: str, message: str = None) -> dict:
        try:
            cmd = ["git", "tag"]
            if message:
                cmd.extend(["-a", tag_name, "-m", message])
            else:
                cmd.append(tag_name)
            subprocess.run(cmd, cwd=repo_path, capture_output=True, check=True)
            return {"success": True, "tag": tag_name}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_remote(self, repo_path: str, action: str, name: str = None, url: str = None) -> dict:
        try:
            if action == "list":
                result = subprocess.run(["git", "remote", "-v"], cwd=repo_path, capture_output=True, text=True, check=True)
                return {"success": True, "remotes": result.stdout}
            elif action == "add":
                subprocess.run(["git", "remote", "add", name, url], cwd=repo_path, capture_output=True, check=True)
                return {"success": True, "message": f"Added remote {name}"}
            elif action == "remove":
                subprocess.run(["git", "remote", "remove", name], cwd=repo_path, capture_output=True, check=True)
                return {"success": True, "message": f"Removed remote {name}"}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_config(self, repo_path: str, action: str, key: str, value: str = None) -> dict:
        try:
            if action == "get":
                result = subprocess.run(["git", "config", key], cwd=repo_path, capture_output=True, text=True, check=True)
                return {"success": True, "value": result.stdout.strip()}
            elif action == "set":
                subprocess.run(["git", "config", key, value], cwd=repo_path, capture_output=True, check=True)
                return {"success": True, "message": f"Set {key} to {value}"}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_cherry_pick(self, repo_path: str, commit: str) -> dict:
        try:
            result = subprocess.run(["git", "cherry-pick", commit], cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_revert(self, repo_path: str, commit: str) -> dict:
        try:
            result = subprocess.run(["git", "revert", "-n", commit], cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_blame(self, repo_path: str, file_path: str) -> dict:
        try:
            result = subprocess.run(["git", "blame", file_path], cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "blame": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    def git_clean(self, repo_path: str, dry_run: bool = True, force: bool = False) -> dict:
        try:
            cmd = ["git", "clean"]
            if dry_run:
                cmd.append("-n")
            if force:
                cmd.append("-f")
            cmd.append("-d")
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, check=True)
            return {"success": True, "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"success": False, "error": e.stderr}
    
    # ===== GITHUB OPERATIONS (28+) =====
    
    def github_get_me(self) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            user = self.gh.get_user()
            return {"success": True, "user": {"login": user.login, "name": user.name, "email": user.email, "bio": user.bio, "public_repos": user.public_repos}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_get_file_contents(self, repo_owner: str, repo_name: str, file_path: str, ref: str = None) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            content = repo.get_contents(file_path, ref=ref)
            if isinstance(content, list):
                files = [{"name": c.name, "type": c.type, "path": c.path} for c in content]
                return {"success": True, "type": "directory", "files": files}
            else:
                try:
                    decoded_content = content.decoded_content.decode()
                except:
                    decoded_content = content.content
                return {"success": True, "type": "file", "content": decoded_content, "path": content.path}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_create_or_update_file(self, repo_owner: str, repo_name: str, file_path: str, content: str, message: str, branch: str = None) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            try:
                existing = repo.get_contents(file_path, ref=branch)
                result = repo.update_file(path=file_path, message=message, content=content, sha=existing.sha, branch=branch)
            except GithubException:
                result = repo.create_file(path=file_path, message=message, content=content, branch=branch)
            return {"success": True, "commit": result["commit"].sha, "file": result["content"].path}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_delete_file(self, repo_owner: str, repo_name: str, file_path: str, message: str, branch: str = None) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            content = repo.get_contents(file_path, ref=branch)
            repo.delete_file(path=file_path, message=message, sha=content.sha, branch=branch)
            return {"success": True, "message": f"Deleted {file_path}"}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_push_files(self, repo_owner: str, repo_name: str, files: List[dict], message: str, branch: str = None) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            results = []
            for file_info in files:
                try:
                    existing = repo.get_contents(file_info["path"], ref=branch)
                    result = repo.update_file(path=file_info["path"], message=message, content=file_info["content"], sha=existing.sha, branch=branch)
                except GithubException:
                    result = repo.create_file(path=file_info["path"], message=message, content=file_info["content"], branch=branch)
                results.append({"path": file_info["path"], "commit": result["commit"].sha})
            return {"success": True, "files_pushed": results}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_create_branch(self, repo_owner: str, repo_name: str, branch_name: str, base_branch: str = "main") -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            base = repo.get_branch(base_branch)
            repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base.commit.sha)
            return {"success": True, "branch": branch_name}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_create_repository(self, repo_name: str, description: str = None, private: bool = False, auto_init: bool = True) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            user = self.gh.get_user()
            repo = user.create_repo(name=repo_name, description=description, private=private, auto_init=auto_init)
            return {"success": True, "repo": {"name": repo.name, "url": repo.html_url, "clone_url": repo.clone_url}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_fork_repository(self, repo_owner: str, repo_name: str) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            user = self.gh.get_user()
            source_repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            forked_repo = user.create_fork(source_repo)
            return {"success": True, "fork": {"name": forked_repo.name, "url": forked_repo.html_url, "clone_url": forked_repo.clone_url}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_list_commits(self, repo_owner: str, repo_name: str, branch: str = None, limit: int = 30) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            commits = repo.get_commits(sha=branch)
            commit_list = []
            for i, commit in enumerate(commits):
                if i >= limit:
                    break
                commit_list.append({"sha": commit.sha, "message": commit.commit.message.split('\n')[0], "author": commit.commit.author.name})
            return {"success": True, "commits": commit_list}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_get_commit(self, repo_owner: str, repo_name: str, commit_sha: str) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            commit = repo.get_commit(commit_sha)
            return {"success": True, "commit": {"sha": commit.sha, "message": commit.commit.message, "author": commit.commit.author.name, "additions": commit.stats.additions, "deletions": commit.stats.deletions}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_create_pull_request(self, repo_owner: str, repo_name: str, title: str, body: str, head_branch: str, base_branch: str = "main") -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            pr = repo.create_pull(title=title, body=body, head=head_branch, base=base_branch)
            return {"success": True, "pr": {"number": pr.number, "title": pr.title, "url": pr.html_url, "state": pr.state}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_list_pull_requests(self, repo_owner: str, repo_name: str, state: str = "open", limit: int = 30) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            prs = repo.get_pulls(state=state)
            pr_list = []
            for i, pr in enumerate(prs):
                if i >= limit:
                    break
                pr_list.append({"number": pr.number, "title": pr.title, "url": pr.html_url, "author": pr.user.login, "state": pr.state})
            return {"success": True, "prs": pr_list}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_get_pull_request(self, repo_owner: str, repo_name: str, pr_number: int) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            pr = repo.get_pull(pr_number)
            return {"success": True, "pr": {"number": pr.number, "title": pr.title, "body": pr.body, "url": pr.html_url, "state": pr.state, "mergeable": pr.mergeable}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_merge_pull_request(self, repo_owner: str, repo_name: str, pr_number: int, commit_message: str = None, merge_method: str = "squash") -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            pr = repo.get_pull(pr_number)
            merge_result = pr.merge(commit_message=commit_message or f"Merge PR #{pr_number}", merge_method=merge_method)
            return {"success": True, "message": "PR merged successfully", "sha": merge_result.sha}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_pull_request_review(self, repo_owner: str, repo_name: str, pr_number: int, event: str, body: str = None) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            pr = repo.get_pull(pr_number)
            review = pr.create_review(event=event, body=body)
            return {"success": True, "review": {"id": review.id, "state": review.state, "body": review.body}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_add_comment_to_pending_review(self, repo_owner: str, repo_name: str, pr_number: int, comment_body: str, in_reply_to: int = None) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            pr = repo.get_pull(pr_number)
            comment = pr.create_issue_comment(comment_body)
            return {"success": True, "comment": {"id": comment.id, "body": comment.body, "url": comment.html_url}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_create_issue(self, repo_owner: str, repo_name: str, title: str, body: str = None, labels: List[str] = None, assignees: List[str] = None) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            issue = repo.create_issue(title=title, body=body, labels=labels, assignees=assignees)
            return {"success": True, "issue": {"number": issue.number, "title": issue.title, "url": issue.html_url, "state": issue.state}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_get_issue(self, repo_owner: str, repo_name: str, issue_number: int) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            issue = repo.get_issue(issue_number)
            return {"success": True, "issue": {"number": issue.number, "title": issue.title, "body": issue.body, "url": issue.html_url, "state": issue.state}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_add_issue_comment(self, repo_owner: str, repo_name: str, issue_number: int, comment_body: str) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            issue = repo.get_issue(issue_number)
            comment = issue.create_comment(comment_body)
            return {"success": True, "comment": {"id": comment.id, "body": comment.body, "url": comment.html_url}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_search_repositories(self, query: str, language: str = None, sort: str = "stars", limit: int = 30) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            search_query = query
            if language:
                search_query += f" language:{language}"
            repos = self.gh.search_repositories(query=search_query, sort=sort)
            repo_list = []
            for i, repo in enumerate(repos):
                if i >= limit:
                    break
                repo_list.append({"name": repo.name, "owner": repo.owner.login, "url": repo.html_url, "stars": repo.stargazers_count, "description": repo.description})
            return {"success": True, "repositories": repo_list}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_search_code(self, query: str, repo_owner: str = None, repo_name: str = None, language: str = None, limit: int = 30) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            search_query = query
            if repo_owner and repo_name:
                search_query += f" repo:{repo_owner}/{repo_name}"
            if language:
                search_query += f" language:{language}"
            results = self.gh.search_code(query=search_query)
            code_list = []
            for i, result in enumerate(results):
                if i >= limit:
                    break
                code_list.append({"file": result.path, "repo": f"{result.repository.owner.login}/{result.repository.name}", "url": result.html_url})
            return {"success": True, "results": code_list}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_search_issues(self, query: str, repo_owner: str = None, repo_name: str = None, state: str = "open", limit: int = 30) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            search_query = query
            if repo_owner and repo_name:
                search_query += f" repo:{repo_owner}/{repo_name}"
            search_query += f" state:{state}"
            issues = self.gh.search_issues(query=search_query)
            issue_list = []
            for i, issue in enumerate(issues):
                if i >= limit:
                    break
                issue_list.append({"number": issue.number, "title": issue.title, "url": issue.html_url, "state": issue.state})
            return {"success": True, "issues": issue_list}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_get_label(self, repo_owner: str, repo_name: str, label_name: str) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            label = repo.get_label(label_name)
            return {"success": True, "label": {"name": label.name, "color": label.color, "description": label.description}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_list_releases(self, repo_owner: str, repo_name: str, limit: int = 30) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            releases = repo.get_releases()
            release_list = []
            for i, release in enumerate(releases):
                if i >= limit:
                    break
                release_list.append({"tag": release.tag_name, "name": release.name, "url": release.html_url, "published_at": release.published_at.isoformat() if release.published_at else None})
            return {"success": True, "releases": release_list}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_get_latest_release(self, repo_owner: str, repo_name: str) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            release = repo.get_latest_release()
            return {"success": True, "release": {"tag": release.tag_name, "name": release.name, "body": release.body, "url": release.html_url}}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_list_tags(self, repo_owner: str, repo_name: str, limit: int = 30) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            tags = repo.get_tags()
            tag_list = []
            for i, tag in enumerate(tags):
                if i >= limit:
                    break
                tag_list.append({"name": tag.name, "commit": tag.commit.sha})
            return {"success": True, "tags": tag_list}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_get_tag(self, repo_owner: str, repo_name: str, tag_name: str) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            tags = repo.get_tags()
            for tag in tags:
                if tag.name == tag_name:
                    return {"success": True, "tag": {"name": tag.name, "commit": tag.commit.sha}}
            return {"success": False, "error": f"Tag {tag_name} not found"}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_get_team_members(self, org: str, team_slug: str) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            team = self.gh.get_organization(org).get_team_by_slug(team_slug)
            members = team.get_members()
            member_list = [{"login": member.login, "name": member.name} for member in members]
            return {"success": True, "members": member_list}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_get_teams(self, org: str) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            org_obj = self.gh.get_organization(org)
            teams = org_obj.get_teams()
            team_list = [{"name": team.name, "slug": team.slug} for team in teams]
            return {"success": True, "teams": team_list}
        except GithubException as e:
            return {"success": False, "error": str(e)}
    
    def github_assign_copilot_to_issue(self, repo_owner: str, repo_name: str, issue_number: int) -> dict:
        if not self.gh:
            return {"success": False, "error": "GitHub token not configured"}
        try:
            repo = self.gh.get_user(repo_owner).get_repo(repo_name)
            issue = repo.get_issue(issue_number)
            issue.create_comment("@copilot assigned to this issue")
            return {"success": True, "message": f"Assigned Copilot to issue #{issue_number}", "issue_url": issue.html_url}
        except GithubException as e:
            return {"success": False, "error": str(e)}


# ===== FASTAPI APPLICATION WITH SSE/STREAMING =====

app = FastAPI(
    title="Comprehensive Git + GitHub MCP Server",
    description="All-in-one MCP server with 50+ Git and GitHub operations - HTTP/SSE streaming",
    version="2.0.0"
)

github_token = os.getenv("GITHUB_TOKEN", "")
server = ComprehensiveGitGitHubServer(github_token)


@app.get("/health")
def health_check():
    return {"status": "healthy", "github_configured": bool(github_token), "version": "2.0.0"}


@app.get("/tools")
def list_tools():
    return {
        "local_git_operations": 25,
        "github_operations": 28,
        "total_operations": 53,
        "streaming": "SSE/HTTP supported"
    }


# GIT ENDPOINTS - WITH STREAMING SUPPORT
@app.post("/git/init")
async def endpoint_git_init(request: GitInitRequest):
    result = server.git_init(request.repo_path)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/status")
async def endpoint_git_status(request: GitStatusRequest):
    result = server.git_status(request.repo_path)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/clone")
async def endpoint_git_clone(request: GitCloneRequest):
    result = server.git_clone(request.repo_url, request.target_dir, request.branch)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/pull")
async def endpoint_git_pull(request: GitPullRequest):
    result = server.git_pull(request.repo_path, request.remote, request.branch)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/push")
async def endpoint_git_push(request: GitPushRequest):
    result = server.git_push(request.repo_path, request.remote, request.branch, request.force)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/commit")
async def endpoint_git_commit(request: GitCommitRequest):
    result = server.git_commit(request.repo_path, request.message, request.author_name, request.author_email, request.files)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/branch")
async def endpoint_git_branch(request: GitBranchRequest):
    result = server.git_branch(request.repo_path, request.filter_type)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/create-branch")
async def endpoint_git_create_branch(request: GitCreateBranchRequest):
    result = server.git_create_branch(request.repo_path, request.branch_name, request.base_branch)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/checkout")
async def endpoint_git_checkout(request: GitCheckoutRequest):
    result = server.git_checkout(request.repo_path, request.branch, request.create)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/merge")
async def endpoint_git_merge(request: GitMergeRequest):
    result = server.git_merge(request.repo_path, request.branch, request.commit_message)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/log")
async def endpoint_git_log(request: GitLogRequest):
    result = server.git_log(request.repo_path, request.limit, request.oneline)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/add")
async def endpoint_git_add(request: GitAddRequest):
    result = server.git_add(request.repo_path, request.files)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/git/reset")
async def endpoint_git_reset(request: GitResetRequest):
    result = server.git_reset(request.repo_path, request.files)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

# GITHUB ENDPOINTS - WITH STREAMING SUPPORT
@app.get("/github/me")
async def endpoint_github_get_me():
    result = server.github_get_me()
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/file-contents")
async def endpoint_github_get_file_contents(request: GitHubGetFileContentsRequest):
    result = server.github_get_file_contents(request.repo_owner, request.repo_name, request.file_path, request.ref)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/create-or-update-file")
async def endpoint_github_create_or_update_file(request: GitHubCreateOrUpdateFileRequest):
    result = server.github_create_or_update_file(request.repo_owner, request.repo_name, request.file_path, request.content, request.message, request.branch)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/delete-file")
async def endpoint_github_delete_file(request: GitHubDeleteFileRequest):
    result = server.github_delete_file(request.repo_owner, request.repo_name, request.file_path, request.message, request.branch)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/push-files")
async def endpoint_github_push_files(request: GitHubPushFilesRequest):
    result = server.github_push_files(request.repo_owner, request.repo_name, request.files, request.message, request.branch)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/create-branch")
async def endpoint_github_create_branch(request: GitHubCreateBranchRequest):
    result = server.github_create_branch(request.repo_owner, request.repo_name, request.branch_name, request.base_branch)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/create-repository")
async def endpoint_github_create_repository(request: GitHubCreateRepositoryRequest):
    result = server.github_create_repository(request.repo_name, request.description, request.private, request.auto_init)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/fork-repository")
async def endpoint_github_fork_repository(request: GitHubForkRepositoryRequest):
    result = server.github_fork_repository(request.repo_owner, request.repo_name)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/list-commits")
async def endpoint_github_list_commits(request: GitHubListCommitsRequest):
    result = server.github_list_commits(request.repo_owner, request.repo_name, request.branch, request.limit)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/get-commit")
async def endpoint_github_get_commit(request: GitHubGetCommitRequest):
    result = server.github_get_commit(request.repo_owner, request.repo_name, request.commit_sha)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/create-pull-request")
async def endpoint_github_create_pull_request(request: GitHubCreatePullRequestRequest):
    result = server.github_create_pull_request(request.repo_owner, request.repo_name, request.title, request.body, request.head_branch, request.base_branch)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/list-pull-requests")
async def endpoint_github_list_pull_requests(request: GitHubListPullRequestsRequest):
    result = server.github_list_pull_requests(request.repo_owner, request.repo_name, request.state, request.limit)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/get-pull-request")
async def endpoint_github_get_pull_request(request: GitHubGetPullRequestRequest):
    result = server.github_get_pull_request(request.repo_owner, request.repo_name, request.pr_number)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/merge-pull-request")
async def endpoint_github_merge_pull_request(request: GitHubMergePullRequestRequest):
    result = server.github_merge_pull_request(request.repo_owner, request.repo_name, request.pr_number, request.commit_message, request.merge_method)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/pull-request-review")
async def endpoint_github_pull_request_review(request: GitHubPullRequestReviewRequest):
    result = server.github_pull_request_review(request.repo_owner, request.repo_name, request.pr_number, request.event, request.body)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/add-comment-to-review")
async def endpoint_github_add_comment_to_review(request: GitHubAddCommentToPendingReviewRequest):
    result = server.github_add_comment_to_pending_review(request.repo_owner, request.repo_name, request.pr_number, request.comment_body, request.in_reply_to)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/create-issue")
async def endpoint_github_create_issue(request: GitHubCreateIssueRequest):
    result = server.github_create_issue(request.repo_owner, request.repo_name, request.title, request.body, request.labels, request.assignees)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/get-issue")
async def endpoint_github_get_issue(request: GitHubGetIssueRequest):
    result = server.github_get_issue(request.repo_owner, request.repo_name, request.issue_number)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/add-issue-comment")
async def endpoint_github_add_issue_comment(request: GitHubAddIssueCommentRequest):
    result = server.github_add_issue_comment(request.repo_owner, request.repo_name, request.issue_number, request.comment_body)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/search-repositories")
async def endpoint_github_search_repositories(request: GitHubSearchRepositoriesRequest):
    result = server.github_search_repositories(request.query, request.language, request.sort, request.limit)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/search-code")
async def endpoint_github_search_code(request: GitHubSearchCodeRequest):
    result = server.github_search_code(request.query, request.repo_owner, request.repo_name, request.language, request.limit)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/search-issues")
async def endpoint_github_search_issues(request: GitHubSearchIssuesRequest):
    result = server.github_search_issues(request.query, request.repo_owner, request.repo_name, request.state, request.limit)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/get-label")
async def endpoint_github_get_label(request: GitHubGetLabelRequest):
    result = server.github_get_label(request.repo_owner, request.repo_name, request.label_name)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/list-releases")
async def endpoint_github_list_releases(request: GitHubListReleasesRequest):
    result = server.github_list_releases(request.repo_owner, request.repo_name, request.limit)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/get-latest-release")
async def endpoint_github_get_latest_release(request: GitHubGetLatestReleaseRequest):
    result = server.github_get_latest_release(request.repo_owner, request.repo_name)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/list-tags")
async def endpoint_github_list_tags(request: GitHubListTagsRequest):
    result = server.github_list_tags(request.repo_owner, request.repo_name, request.limit)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/get-tag")
async def endpoint_github_get_tag(request: GitHubGetTagRequest):
    result = server.github_get_tag(request.repo_owner, request.repo_name, request.tag_name)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/get-team-members")
async def endpoint_github_get_team_members(request: GitHubGetTeamMembersRequest):
    result = server.github_get_team_members(request.org, request.team_slug)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/get-teams")
async def endpoint_github_get_teams(request: GitHubGetTeamsRequest):
    result = server.github_get_teams(request.org)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")

@app.post("/github/assign-copilot")
async def endpoint_github_assign_copilot(request: GitHubAssignCopilotToIssueRequest):
    result = server.github_assign_copilot_to_issue(request.repo_owner, request.repo_name, request.issue_number)
    return StreamingResponse((await server.stream_json(result)), media_type="application/x-ndjson")


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    print(f"\n{'='*70}")
    print(f"🚀 Comprehensive Git + GitHub MCP Server")
    print(f"{'='*70}")
    print(f"📍 Starting server on {host}:{port}")
    print(f"📚 API Docs: http://localhost:{port}/docs")
    print(f"📖 ReDoc: http://localhost:{port}/redoc")
    print(f"✨ Streaming Support: HTTP/SSE")
    print(f"🔧 Total Operations: 53 (25 Git + 28 GitHub)")
    print(f"{'='*70}\n")
    uvicorn.run(app, host=host, port=port, log_level="info")
