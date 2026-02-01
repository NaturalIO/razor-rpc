# General
- If you don't know 3rd-party API, should lookup on `https://docs.rs/<crate>`.

# Code style
- Importing the names at file level
- Do not import at function level
- All comment must be english
- Write comment only when necessary, avoid stupit obvious lines, avoid multi-lines comment block unless written by user.

# Document
- Must be concise, well organize into catelog, no duplicated topic, no redundant information. Relative topic should be organize in near position.

# Workflow

- Follow instructions, do not deviate from the topic
- Make sure the code builds
- When working on a test case, run test with `make test <test_name>` to prevent output truncate
- run all tests with `make test`
- Fix all cargo warning
- Fix cargo clippy warning on the files edited
