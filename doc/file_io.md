<!--
SPDX-FileCopyrightText: 2023 Changgyoo Park <wvwwvwwv@me.com>

SPDX-License-Identifier: Apache-2.0
-->

# File IO Persistence Layer

* WORK_IN_PROGRESS.

The file IO persistence layer is provided as the default persistence layer implementation. The module heavily relies on [io_uring](https://en.wikipedia.org/wiki/Io_uring) for asynchronous user space IO operations.

## Page

The size of a page is `512B`.
