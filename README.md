# Null String filter plugin for Embulk

A filter plugin for Embulk to replace NULL string to null.

## Overview

* **Plugin type**: filter

## Configuration

- **columns**: name of convert columns
-   **name**: column name (string, required)
-   **null_string**: value of null_string (string, required)

## Example

```yaml
- type: null_string
  columns:
  - name: comment
    null_string: ""
```

```yaml
- type: null_string
  columns:
  - name: comment
    null_string: "\\N"
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
