# Copyright 2026 Dennis Lanov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class ExecutionOwnerAlreadyHeld(Exception):
    """Another process or instance already holds this execution-owner lock."""

    def __init__(self, canonical_db_path: str, lock_path: str) -> None:
        self.canonical_db_path = canonical_db_path
        self.lock_path = lock_path
        super().__init__(
            f'execution owner lock already held for {canonical_db_path!r} '
            f'(lock file {lock_path!r})')
