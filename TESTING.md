# Running tests

This repository includes simple unit tests for both the Python backend and the React frontend.

## Backend (Python)

1. Install dependencies (if not already):

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
# Install test runner
python -m pip install pytest
```

2. Run tests from the repository root:

```bash
pytest -q
```

The CI workflow runs the same tests on every push/PR.

## Frontend (React)

1. Install node dependencies:

```bash
cd frontend/react-app
npm ci
```

2. Run frontend unit tests locally:

```bash
npm test
```

The CI workflow will also run the frontend tests during PRs/pushes.

## Check CI results

- Open the **Actions** tab in your GitHub repository to see CI runs and logs.
- The `CI` workflow runs on push and pull requests and will show test results.

If you want, I can add coverage reporting and fail the CI on low coverage as a follow-up.